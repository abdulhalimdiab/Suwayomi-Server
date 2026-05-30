package suwayomi.tachidesk.server.util

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import android.app.Application
import android.content.Context
import eu.kanade.tachiyomi.network.GET
import eu.kanade.tachiyomi.network.NetworkHelper
import eu.kanade.tachiyomi.network.awaitSuccess
import eu.kanade.tachiyomi.util.lang.launchIO
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.reactivecircus.cache4k.Cache
import io.javalin.config.JavalinConfig
import io.javalin.http.staticfiles.AliasCheck
import io.javalin.http.staticfiles.Location
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.sample
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import net.lingala.zip4j.ZipFile
import suwayomi.tachidesk.graphql.types.AboutWebUI
import suwayomi.tachidesk.graphql.types.UpdateState
import suwayomi.tachidesk.graphql.types.UpdateState.DOWNLOADING
import suwayomi.tachidesk.graphql.types.UpdateState.ERROR
import suwayomi.tachidesk.graphql.types.UpdateState.FINISHED
import suwayomi.tachidesk.graphql.types.UpdateState.IDLE
import suwayomi.tachidesk.graphql.types.WebUIChannel
import suwayomi.tachidesk.graphql.types.WebUIFlavor
import suwayomi.tachidesk.graphql.types.WebUIUpdateInfo
import suwayomi.tachidesk.graphql.types.WebUIUpdateStatus
import suwayomi.tachidesk.server.ApplicationDirs
import suwayomi.tachidesk.server.generated.BuildConfig
import suwayomi.tachidesk.server.serverConfig
import suwayomi.tachidesk.server.util.ExitCode.WebUISetupFailure
import suwayomi.tachidesk.util.HAScheduler
import uy.kohesive.injekt.Injekt
import uy.kohesive.injekt.api.get
import uy.kohesive.injekt.injectLazy
import java.io.File
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.URI
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Date
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

private val applicationDirs: ApplicationDirs by injectLazy()
private val tmpDir = System.getProperty("java.io.tmpdir")

private fun ByteArray.toHex(): String = joinToString(separator = "") { eachByte -> "%02x".format(eachByte) }

class BundledWebUIMissing : Exception("No bundled webUI version found")

fun WebUIFlavor.isDefault(): Boolean = this == WebUIFlavor.default

object WebInterfaceManager {
    private val logger = KotlinLogging.logger {}
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    private const val LAST_WEBUI_UPDATE_CHECK_KEY = "lastWebUIUpdateCheck"
    private const val SERVED_WEBUI_FLAVOR_KEY = "servedWebUIFlavor"
    private const val VERSION_UPDATE_TIMESTAMP_KEY = "webUIVersionUpdateTimestamp"

    private val preferences = Injekt.get<Application>().getSharedPreferences("server_util", Context.MODE_PRIVATE)
    private var currentUpdateTaskId: String = ""

    val isSetupComplete = MutableStateFlow(false)

    private val json: Json by injectLazy()
    private val network: NetworkHelper by injectLazy()

    private val CACHE_DURATION = 5.minutes
    private val versionMappingCache = Cache.Builder<String, JsonArray>().expireAfterWrite(CACHE_DURATION).build()
    private val previewVersionCache = Cache.Builder<String, String>().expireAfterWrite(CACHE_DURATION).build()

    private val notifyFlow = MutableSharedFlow<WebUIUpdateStatus?>()

    private val statusFlow = MutableSharedFlow<WebUIUpdateStatus>()
    val status =
        statusFlow.stateIn(
            scope,
            SharingStarted.Eagerly,
            getStatus(),
        )

    init {
        scope.launch {
            @OptIn(FlowPreview::class)
            notifyFlow.sample(1.seconds).collect {
                if (it != null) {
                    logger.debug { "notifyFlow: sampling $it" }
                    statusFlow.emit(it)
                }
            }
        }

        serverConfig.subscribeTo(
            combine(serverConfig.webUIUpdateCheckInterval, serverConfig.webUIFlavor) { interval, flavor ->
                Pair(
                    interval,
                    flavor,
                )
            },
            ::scheduleWebUIUpdateCheck,
            ignoreInitialValue = false,
        )
    }

    fun getAboutInfo(): AboutWebUI {
        val currentVersion = getLocalVersion()

        val failedToGetVersion = currentVersion === "r-1"
        if (failedToGetVersion) {
            throw Exception("Failed to get current version")
        }

        return AboutWebUI(
            channel = serverConfig.webUIChannel.value,
            tag = currentVersion,
            updateTimestamp = preferences.getLong(VERSION_UPDATE_TIMESTAMP_KEY, System.currentTimeMillis()),
        )
    }

    fun getStatus(
        version: String = "",
        state: UpdateState = IDLE,
        progress: Int = 0,
    ): WebUIUpdateStatus =
        WebUIUpdateStatus(
            info =
                WebUIUpdateInfo(
                    channel = serverConfig.webUIChannel.value,
                    tag = version,
                ),
            state,
            progress,
        )

    fun resetStatus() {
        emitStatus("", IDLE, 0, immediate = true)
    }

    private var serveWebUI: () -> Unit = {}

    fun setup(config: JavalinConfig) {
        if (!serverConfig.webUIEnabled.value) {
            return
        }

        File(applicationDirs.webUIServe).mkdirs()

        config.staticFiles.add { staticFiles ->
            if (ServerSubpath.isDefined()) staticFiles.hostedPath = ServerSubpath.normalized()
            // Use canonical path to avoid Jetty alias issues
            staticFiles.directory = File(applicationDirs.webUIServe).canonicalPath
            staticFiles.location = Location.EXTERNAL
            staticFiles.aliasCheck = AliasCheck { _, _ -> true }
        }

        serveWebUI = {
            val updatedServableRoot = createServableRoot()
            config.spaRoot.addFile(ServerSubpath.asRootPath(), "$updatedServableRoot/index.html", Location.EXTERNAL)

            logger.info {
                "Serving SPA files for ${serverConfig.webUIFlavor.value}" +
                    if (ServerSubpath.isDefined()) " under subpath '${ServerSubpath.normalized()}'" else ""
            }
        }

        @OptIn(DelicateCoroutinesApi::class)
        GlobalScope.launchIO {
            setupWebUI()
            isSetupComplete.value = true
        }
    }

    private fun createServableRoot(): String {
        val tempWebUIRoot = createServableDirectory()
        val orgIndexHtml = File("$tempWebUIRoot/index.html")

        if (orgIndexHtml.exists()) {
            val originalIndexHtml = orgIndexHtml.readText()

            var modifiedIndexHtml = originalIndexHtml

            if (ServerSubpath.isDefined()) {
                val subpathInjectionBaseTag = "<base href=\"${ServerSubpath.asRootPath()}\">"
                modifiedIndexHtml =
                    modifiedIndexHtml.replace(
                        "<head>",
                        "<head>$subpathInjectionBaseTag",
                    )
            }

            val shutdownButtonInjection = """
<script>
(function() {
    console.log('[sm] init');
    var apiBase = '${ServerSubpath.maybeAddAsPrefix("/api/v1/server")}';
    if (!apiBase.startsWith('/')) apiBase = '/' + apiBase;
    var shutdownApi = apiBase + '/shutdown';
    var restartApi = apiBase + '/restart';

    function addStyle(css) {
        var s = document.createElement('style');
        s.textContent = css;
        document.head.appendChild(s);
    }

    addStyle('#sm-modal{display:none;position:fixed;inset:0;z-index:2147483647;background:rgba(0,0,0,0.5);align-items:center;justify-content:center}#sm-modal.show{display:flex}#sm-modal .sm-content{background:#222635;color:#fff;padding:32px;border-radius:12px;max-width:400px;width:90%;text-align:center;box-shadow:0 8px 32px rgba(0,0,0,0.5);font:400 16px/1.5 Roboto,Helvetica,Arial,sans-serif}#sm-modal .sm-content h3{margin:0 0 12px;font-size:20px}#sm-modal .sm-content p{margin:0 0 24px;color:rgba(255,255,255,0.7);font-size:14px}#sm-modal .sm-actions{display:flex;gap:12px;justify-content:center}#sm-modal .sm-actions button{all:unset;padding:8px 24px;border-radius:4px;cursor:pointer;text-transform:uppercase;letter-spacing:.02857em;font-weight:500;font-size:14px;line-height:1.75}#sm-modal .sm-actions .sm-cancel{background:rgba(255,255,255,0.1);color:#fff}#sm-modal .sm-actions .sm-cancel:hover{background:rgba(255,255,255,0.2)}#sm-modal .sm-actions .sm-confirm-shutdown{background:#c62828;color:#fff}#sm-modal .sm-actions .sm-confirm-shutdown:hover{background:#e53935}#sm-modal .sm-actions .sm-confirm-restart{background:#1e88e5;color:#fff}#sm-modal .sm-actions .sm-confirm-restart:hover{background:#32a0ff}#sm-overlay{display:none;position:fixed;inset:0;z-index:2147483647;background:#0c1021;color:#fff;flex-direction:column;align-items:center;justify-content:center;font:400 16px/1.5 Roboto,Helvetica,Arial,sans-serif;gap:16px;text-align:center;padding:20px}#sm-overlay.show{display:flex}#sm-overlay h2{font-size:24px;margin:0}#sm-overlay p{margin:0;color:rgba(255,255,255,0.7)}');

    var modalHtml = '<div id="sm-modal"><div class="sm-content"><h3 id="sm-title">Confirm</h3><p id="sm-msg">Are you sure?</p><div class="sm-actions"><button class="sm-cancel" id="sm-cancel">Cancel</button><button id="sm-confirm" class="sm-confirm-shutdown">Confirm</button></div></div></div>';
    var overlayHtml = '<div id="sm-overlay"><svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="12" r="10"/><path d="M12 8v4M12 16h.01"/></svg><h2 id="sm-overlay-title">Server Offline</h2><p id="sm-overlay-msg">The server has been shut down.</p></div>';

    function initUI() {
        if (document.getElementById('sm-modal') || document.getElementById('sm-overlay')) return;
        var d1 = document.createElement('div'); d1.innerHTML = modalHtml; document.body.appendChild(d1.firstElementChild);
        var d2 = document.createElement('div'); d2.innerHTML = overlayHtml; document.body.appendChild(d2.firstElementChild);
    }
    initUI();

    var modal = document.getElementById('sm-modal');
    var modalTitle = document.getElementById('sm-title');
    var modalMsg = document.getElementById('sm-msg');
    var modalConfirm = document.getElementById('sm-confirm');
    var overlay = document.getElementById('sm-overlay');
    var overlayTitle = document.getElementById('sm-overlay-title');
    var overlayMsg = document.getElementById('sm-overlay-msg');

    var cancelBtn = document.getElementById('sm-cancel');
    if (cancelBtn) cancelBtn.onclick = function() { modal.classList.remove('show'); };

    function showModal(action, onConfirm) {
        if (action === 'shutdown') {
            modalTitle.textContent = 'Shutdown Server';
            modalMsg.textContent = 'Are you sure you want to shut down the server? All ongoing operations will be stopped.';
            modalConfirm.textContent = 'Shutdown';
            modalConfirm.className = 'sm-confirm-shutdown';
        } else {
            modalTitle.textContent = 'Restart Server';
            modalMsg.textContent = 'Are you sure you want to restart the server? It will be unavailable for a short moment.';
            modalConfirm.textContent = 'Restart';
            modalConfirm.className = 'sm-confirm-restart';
        }
        modalConfirm.onclick = function() {
            modal.classList.remove('show');
            onConfirm();
        };
        modal.classList.add('show');
    }

    function showOverlay(action) {
        if (action === 'shutdown') {
            overlayTitle.textContent = 'Server Shut Down';
            overlayMsg.textContent = 'The server has been shut down. You may close this page.';
        } else {
            overlayTitle.textContent = 'Server Restarting';
            overlayMsg.textContent = 'The server is restarting. Please wait a moment then reload.';
        }
        overlay.classList.add('show');
    }

    function runAction(action) {
        var api = action === 'shutdown' ? shutdownApi : restartApi;
        showModal(action, function() {
            fetch(api, {method:'POST'}).then(function() { showOverlay(action); }).catch(function() { showOverlay(action); });
        });
    }

    var navIcons = {
        shutdown: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18.36 6.64a9 9 0 1 1-12.73 0"/><line x1="12" y1="2" x2="12" y2="12"/></svg>',
        restart: '<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>'
    };

    var injected = false;

    function findItem(text) {
        var all = document.querySelectorAll('a, span, [class*="MuiListItemButton"]');
        var lower = text.toLowerCase();
        for (var i = 0; i < all.length; i++) {
            if (all[i].textContent.trim().toLowerCase() === lower) {
                console.log('[sm] found', text, ':', all[i]);
                return all[i];
            }
        }
        console.log('[sm] NOT found:', text);
        return null;
    }

    function injectSidebar() {
        var about = findItem('About');
        if (!about) { injected = false; console.log('[sm] About not found'); return false; }
        if (injected) return true;
        var list = about.parentElement;
        if (!list) { console.log('[sm] About has no parent'); return false; }
        var ref = about.nextElementSibling;

        var sep = document.createElement('a');
        sep.style.cssText = 'display:block;height:1px;margin:8px 16px;border-top:1px solid rgba(255,255,255,0.12);pointer-events:none;cursor:default';
        list.insertBefore(sep, ref);

        ['Shutdown', 'Restart'].forEach(function(label) {
            var clone = about.cloneNode(true);
            clone.removeAttribute('href');
            clone.addEventListener('click', function(e) { e.preventDefault(); e.stopPropagation(); runAction(label.toLowerCase()); });
            var icon = clone.querySelector('[class*="MuiListItemIcon"]');
            if (icon) icon.innerHTML = navIcons[label.toLowerCase()];
            var text = clone.querySelector('[class*="MuiListItemText"]');
            if (text) (text.querySelector('[class*="primary"]') || text).textContent = label;
            list.insertBefore(clone, ref);
        });

        injected = true;
        console.log('[sm] sidebar injection success');
        return true;
    }

    function tryInject() {
        injectSidebar();
        var obs = new MutationObserver(function() {
            injectSidebar();
        });
        obs.observe(document.body, {childList:true, subtree:true});
        setTimeout(function() {
            if (!injected) {
                console.log('[sm] sidebar failed, showing floating buttons');
                addStyle('#sm-fb-wrap{position:fixed;bottom:24px;left:24px;z-index:9999;display:flex;flex-direction:column;gap:12px}#sm-fb-wrap button{width:56px;height:56px;border:none;border-radius:16px;cursor:pointer;display:flex;align-items:center;justify-content:center;box-shadow:0 3px 5px -1px rgba(0,0,0,0.2),0 6px 10px 0 rgba(0,0,0,0.14),0 1px 18px 0 rgba(0,0,0,0.12)}#sm-fb-wrap button svg{width:24px;height:24px}#sm-fb-wrap .sm-fb-restart{background:#1565c0;color:#fff}#sm-fb-wrap .sm-fb-shutdown{background:#c62828;color:#fff}');
                var w = document.createElement('div'); w.id = 'sm-fb-wrap';
                var r = document.createElement('button'); r.className = 'sm-fb-restart'; r.innerHTML = navIcons.restart; r.title = 'Restart'; r.onclick = function() { runAction('restart'); };
                var s = document.createElement('button'); s.className = 'sm-fb-shutdown'; s.innerHTML = navIcons.shutdown; s.title = 'Shutdown'; s.onclick = function() { runAction('shutdown'); };
                w.appendChild(r); w.appendChild(s); document.body.appendChild(w);
            }
        }, 8000);
    }

    if (document.body) tryInject();
    else document.addEventListener('DOMContentLoaded', tryInject);
})();
</script>"""
            modifiedIndexHtml = modifiedIndexHtml.replace("</body>", "$shutdownButtonInjection</body>")

            orgIndexHtml.writeText(modifiedIndexHtml)
        }

        return tempWebUIRoot
    }

    private fun createServableDirectory(): String {
        val originalWebUIRoot = applicationDirs.webUIRoot
        val tempWebUIRoot = applicationDirs.webUIServe

        File(tempWebUIRoot).deleteRecursively()
        File(tempWebUIRoot).mkdirs()

        File(originalWebUIRoot).copyRecursively(File(tempWebUIRoot), overwrite = true)

        logger.debug { "Created servable WebUI directory at: $tempWebUIRoot" }

        // Return canonical path to avoid Jetty alias issues
        return File(tempWebUIRoot).canonicalPath
    }

    private fun updateServedWebUIInfo(flavor: WebUIFlavor) {
        preferences.edit().putString(SERVED_WEBUI_FLAVOR_KEY, flavor.uiName).apply()
        preferences.edit().putLong(VERSION_UPDATE_TIMESTAMP_KEY, System.currentTimeMillis()).apply()
    }

    private fun getServedWebUIFlavor(): WebUIFlavor =
        WebUIFlavor.from(preferences.getString(SERVED_WEBUI_FLAVOR_KEY, WebUIFlavor.default.uiName)!!)

    private fun isAutoUpdateEnabled(): Boolean = serverConfig.webUIUpdateCheckInterval.value.toInt() > 0

    private fun scheduleWebUIUpdateCheck() {
        HAScheduler.descheduleCron(currentUpdateTaskId)

        val isAutoUpdateDisabled = !isAutoUpdateEnabled() || serverConfig.webUIFlavor.value == WebUIFlavor.CUSTOM
        if (isAutoUpdateDisabled) {
            return
        }

        val updateInterval =
            serverConfig.webUIUpdateCheckInterval.value.hours
                .coerceAtLeast(1.hours)
                .coerceAtMost(23.hours)
        val lastAutomatedUpdate = preferences.getLong(LAST_WEBUI_UPDATE_CHECK_KEY, System.currentTimeMillis())

        val task = {
            if (isSetupComplete.value) {
                val log =
                    KotlinLogging.logger(
                        "${logger.name}::scheduleWebUIUpdateCheck(" +
                            "flavor= ${WebUIFlavor.current.uiName}, " +
                            "channel= ${serverConfig.webUIChannel.value}, " +
                            "interval= ${serverConfig.webUIUpdateCheckInterval.value}h, " +
                            "lastAutomatedUpdate= ${
                                Date(
                                    lastAutomatedUpdate,
                                )
                            })",
                    )
                log.debug { "called" }

                runBlocking {
                    try {
                        checkForUpdate(WebUIFlavor.current)
                    } catch (e: Exception) {
                        log.error(e) { "failed due to" }
                    }
                }
            }
        }

        val wasPreviousUpdateCheckTriggered =
            (System.currentTimeMillis() - lastAutomatedUpdate) < updateInterval.inWholeMilliseconds
        if (!wasPreviousUpdateCheckTriggered) {
            @OptIn(DelicateCoroutinesApi::class)
            GlobalScope.launch(Dispatchers.IO) {
                task()
            }
        }

        currentUpdateTaskId =
            HAScheduler.scheduleCron(task, "0 */${updateInterval.inWholeHours} * * *", "webUI-update-checker")
    }

    suspend fun setupWebUI() {
        if (serverConfig.webUIFlavor.value == WebUIFlavor.CUSTOM) {
            serveWebUI()
            return
        }

        val servedFlavor = getServedWebUIFlavor()

        val log =
            KotlinLogging.logger(
                "${logger.name} setupWebUI(flavor= ${WebUIFlavor.current.uiName}, servedFlavor= ${servedFlavor.uiName}, channel= ${serverConfig.webUIChannel})",
            )

        val flavor =
            if (serverConfig.webUIChannel.value == WebUIChannel.BUNDLED) {
                if (serverConfig.webUIFlavor.value != WebUIFlavor.default) {
                    log.warn {
                        "Changed flavor to ${WebUIFlavor.default.uiName}. Channel \"${WebUIChannel.BUNDLED}\" only works with the default flavor"
                    }
                }

                WebUIFlavor.default
            } else {
                WebUIFlavor.current
            }

        if (doesLocalWebUIExist(applicationDirs.webUIRoot)) {
            val currentVersion = getLocalVersion()

            log.info { "found webUI files - version= $currentVersion" }
            serveWebUI()

            val hasFlavorChanged = flavor.uiName != servedFlavor.uiName
            if (hasFlavorChanged) {
                try {
                    doInitialSetup(flavor)
                    serveWebUI()
                    return
                } catch (e: Exception) {
                    log.warn(e) { "Failed to install the version of the new flavor, proceeding with version of previous flavor" }
                }
            }

            val flavorToValidate = if (hasFlavorChanged) servedFlavor else flavor
            if (!isLocalWebUIValid(flavorToValidate, applicationDirs.webUIRoot)) {
                try {
                    doInitialSetup(flavorToValidate, isInvalid = true)
                    serveWebUI()
                } catch (e: Exception) {
                    log.warn(e) { "WebUI is invalid and failed to install a valid version, proceeding with invalid version" }
                }
                return
            }

            if (isAutoUpdateEnabled()) {
                checkForUpdate(flavor)
            }

            // check if the bundled webUI version is a newer version than the current used version
            // this could be the case in case no compatible webUI version is available and a newer server version was installed
            val shouldUpdateToBundledVersion =
                flavor.isDefault() && extractVersion(getLocalVersion()) < extractVersion(BuildConfig.WEBUI_TAG)
            if (shouldUpdateToBundledVersion) {
                log.debug { "update to bundled version \"${BuildConfig.WEBUI_TAG}\"" }

                try {
                    setupBundledWebUI()
                    serveWebUI()
                } catch (e: Exception) {
                    log.error(e) { "failed the update to the bundled webUI" }
                }
            }

            return
        }

        log.warn { "no webUI files found, starting download..." }
        try {
            doInitialSetup(flavor)
            serveWebUI()
        } catch (e: Exception) {
            log.error(e) {
                "Failed to setup the webUI. Unable to start the server with a served webUI, change the settings to start" +
                    "without one. Stopping the server now..."
            }
            shutdownApp(WebUISetupFailure)
        }
    }

    /**
     * Tries to download the latest compatible version for the selected webUI and falls back to the default webUI in case of errors.
     */
    private suspend fun doInitialSetup(
        flavor: WebUIFlavor,
        isInvalid: Boolean = false,
    ) {
        val log =
            KotlinLogging.logger("${logger.name} doInitialSetup(flavor= ${flavor.uiName})")

        val isLocalWebUIValid = !isInvalid && isLocalWebUIValid(flavor, applicationDirs.webUIRoot)

        /**
         * Performs the download and returns if the download was successful.
         *
         * In case the download failed but the local webUI is valid the download is considered a success to prevent the fallback logic
         */
        val doDownload: suspend (getVersion: suspend () -> String) -> Boolean = { getVersion ->
            try {
                downloadVersion(flavor, getVersion())
                true
            } catch (_: Exception) {
                false
            } ||
                isLocalWebUIValid
        }

        // download the latest compatible version for the current selected webUI
        val fallbackToDefaultWebUI = !doDownload { getLatestCompatibleVersion(flavor) }
        if (!fallbackToDefaultWebUI) {
            return
        }

        if (!flavor.isDefault()) {
            log.warn { "fallback to default webUI \"${WebUIFlavor.default.uiName}\"" }

            serverConfig.webUIFlavor.value = WebUIFlavor.default

            val fallbackToBundledVersion = !doDownload { getLatestCompatibleVersion(flavor) }
            if (!fallbackToBundledVersion) {
                return
            }
        }

        log.warn { "fallback to bundled default webUI \"${WebUIFlavor.default.uiName}\"" }

        try {
            setupBundledWebUI()
        } catch (_: Exception) {
            throw Exception("Unable to setup a webUI")
        }
    }

    private suspend fun setupBundledWebUI() {
        try {
            extractBundledWebUI()
            updateServedWebUIInfo(WebUIFlavor.default)
            return
        } catch (e: BundledWebUIMissing) {
            logger.warn(e) { "setupBundledWebUI: fallback to downloading the version of the bundled webUI" }
        }

        downloadVersion(WebUIFlavor.default, BuildConfig.WEBUI_TAG)
    }

    private fun extractBundledWebUI() {
        val resourceWebUI: InputStream =
            BuildConfig::class.java.getResourceAsStream("/WebUI.zip") ?: throw BundledWebUIMissing()

        logger.info { "extractBundledWebUI: Using the bundled WebUI zip..." }

        val webUIZip = WebUIFlavor.default.baseFileName
        val webUIZipPath = "$tmpDir/$webUIZip"
        val webUIZipFile = File(webUIZipPath)
        resourceWebUI.use { input ->
            webUIZipFile.outputStream().use { output ->
                input.copyTo(output)
            }
        }

        File(applicationDirs.webUIRoot).deleteRecursively()
        extractDownload(webUIZipPath, applicationDirs.webUIRoot)
    }

    private suspend fun checkForUpdate(flavor: WebUIFlavor) {
        preferences.edit().putLong(LAST_WEBUI_UPDATE_CHECK_KEY, System.currentTimeMillis()).apply()
        val localVersion = getLocalVersion()

        val log =
            KotlinLogging.logger("${logger.name} checkForUpdate(flavor= ${flavor.uiName}, localVersion= $localVersion)")

        if (!isUpdateAvailable(flavor, localVersion).second) {
            log.debug { "local version is the latest one" }
            return
        }

        log.info { "An update is available, starting download..." }
        try {
            downloadVersion(flavor, getLatestCompatibleVersion(flavor))
            serveWebUI()
        } catch (e: Exception) {
            log.warn(e) { "failed due to" }
        }
    }

    private fun getDownloadUrlFor(
        flavor: WebUIFlavor,
        version: String,
    ): String {
        val baseReleasesUrl = "${flavor.repoUrl}/releases"
        val downloadSpecificVersionBaseUrl = "$baseReleasesUrl/download"

        return "$downloadSpecificVersionBaseUrl/$version"
    }

    private fun getLocalVersion(path: String = applicationDirs.webUIRoot): String =
        try {
            File("$path/revision").readText().trim()
        } catch (_: Exception) {
            "r-1"
        }

    private fun doesLocalWebUIExist(path: String): Boolean {
        // check if we have webUI installed and is correct version
        val webUIRevisionFile = File("$path/revision")
        return webUIRevisionFile.exists()
    }

    private suspend fun isLocalWebUIValid(
        flavor: WebUIFlavor,
        path: String,
    ): Boolean {
        if (!doesLocalWebUIExist(path)) {
            return false
        }

        val log =
            KotlinLogging.logger("${logger.name} isLocalWebUIValid(flavor= ${flavor.uiName}, path= $path)")
        log.info { "Verifying WebUI files..." }

        val currentVersion = getLocalVersion(path)
        val localMD5Sum = getLocalMD5Sum(path)
        val currentVersionMD5Sum = fetchMD5SumFor(flavor, currentVersion)
        val validationSucceeded = currentVersionMD5Sum == localMD5Sum

        log.info {
            "Validation " +
                "${if (validationSucceeded) "succeeded" else "failed"} - " +
                "md5: local= $localMD5Sum; expected= $currentVersionMD5Sum"
        }

        return validationSucceeded
    }

    private fun getLocalMD5Sum(fileDir: String): String {
        var sum = ""
        File(fileDir).walk().toList().sortedBy { it.path }.forEach { file ->
            if (file.isFile) {
                val md5 = MessageDigest.getInstance("MD5")
                md5.update(file.readBytes())
                val digest = md5.digest()
                sum += digest.toHex()
            }
        }

        val md5 = MessageDigest.getInstance("MD5")
        md5.update(sum.toByteArray(StandardCharsets.UTF_8))
        val digest = md5.digest()
        return digest.toHex()
    }

    private suspend fun <T> executeWithRetry(
        log: KLogger,
        execute: suspend () -> T,
        maxRetries: Int = 3,
        retryCount: Int = 0,
        timeout: Duration = 2.seconds,
    ): T {
        try {
            return execute()
        } catch (e: Exception) {
            log.warn(e) { "(retry $retryCount/$maxRetries) failed due to" }

            if (retryCount < maxRetries) {
                delay(timeout.times(retryCount + 1))
                return executeWithRetry(log, execute, maxRetries, retryCount + 1)
            }

            throw e
        }
    }

    private suspend fun fetchMD5SumFor(
        flavor: WebUIFlavor,
        version: String,
    ): String =
        try {
            executeWithRetry(KotlinLogging.logger("${logger.name} fetchMD5SumFor(flavor= ${flavor.uiName}, version= $version)"), {
                network.client
                    .newCall(GET("${getDownloadUrlFor(flavor, version)}/md5sum"))
                    .awaitSuccess()
                    .body
                    .string()
                    .trim()
            })
        } catch (_: Exception) {
            ""
        }

    private fun extractVersion(versionString: String): Int {
        // version string is of format "r<number>"
        return versionString.substring(1).toInt()
    }

    private suspend fun fetchPreviewVersion(flavor: WebUIFlavor): String =
        previewVersionCache.get(flavor.uiName) {
            executeWithRetry(
                KotlinLogging.logger("${logger.name} fetchPreviewVersion(${flavor.uiName})"),
                {
                    val releaseInfoJson =
                        network.client
                            .newCall(GET(flavor.latestReleaseInfoUrl))
                            .awaitSuccess()
                            .body
                            .string()
                    Json.decodeFromString<JsonObject>(releaseInfoJson)["tag_name"]?.jsonPrimitive?.content
                        ?: throw Exception("Failed to get the preview version tag")
                },
            )
        }

    private suspend fun fetchServerMappingFile(flavor: WebUIFlavor): JsonArray =
        versionMappingCache.get(flavor.uiName) {
            executeWithRetry(
                KotlinLogging.logger("$logger fetchServerMappingFile(${flavor.uiName})"),
                {
                    json
                        .parseToJsonElement(
                            network.client
                                .newCall(GET(flavor.versionMappingUrl))
                                .awaitSuccess()
                                .body
                                .string(),
                        ).jsonArray
                },
            )
        }

    private suspend fun getLatestCompatibleVersion(flavor: WebUIFlavor): String {
        if (serverConfig.webUIChannel.value == WebUIChannel.BUNDLED) {
            logger.debug { "getLatestCompatibleVersion: Channel is \"${WebUIChannel.BUNDLED}\", do not check for update" }
            return BuildConfig.WEBUI_TAG
        }

        val currentServerVersionNumber =
            BuildConfig.VERSION
                .split(".")
                .last()
                .toInt()
        val webUIToServerVersionMappings = fetchServerMappingFile(flavor)

        logger.debug {
            "getLatestCompatibleVersion: " +
                "flavor= ${flavor.uiName}, " +
                "webUIChannel= ${serverConfig.webUIChannel.value}, " +
                "currentServerVersion= ${BuildConfig.VERSION}, " +
                "mappingFile= $webUIToServerVersionMappings"
        }

        for (i in 0 until webUIToServerVersionMappings.size) {
            val webUIToServerVersionEntry = webUIToServerVersionMappings[i].jsonObject
            var webUIVersion =
                webUIToServerVersionEntry["uiVersion"]?.jsonPrimitive?.content
                    ?: throw Exception("Invalid mappingFile")
            val minServerVersionString =
                webUIToServerVersionEntry["serverVersion"]
                    ?.jsonPrimitive
                    ?.content
                    ?: throw Exception("Invalid mappingFile")
            val minServerVersionNumber = extractVersion(minServerVersionString)

            // is a STABLE webUI release, without a specified webUI version, which requires same handling as the PREVIEW release
            val isUnknownStableVersion = webUIVersion == "STABLEPREVIEW"

            if (serverConfig.webUIChannel.value != WebUIChannel.from(webUIVersion)) {
                // allow only STABLE versions for STABLE channel
                if (serverConfig.webUIChannel.value == WebUIChannel.STABLE && !isUnknownStableVersion) {
                    continue
                }

                // allow all versions for PREVIEW channel
            }

            if (webUIVersion == WebUIChannel.PREVIEW.name || isUnknownStableVersion) {
                webUIVersion = fetchPreviewVersion(flavor)
            }

            val isNewerThanBundled =
                !flavor.isDefault() || extractVersion(webUIVersion) >= extractVersion(BuildConfig.WEBUI_TAG)
            val isCompatibleVersion = minServerVersionNumber <= currentServerVersionNumber && isNewerThanBundled
            if (isCompatibleVersion) {
                return webUIVersion
            }
        }

        throw Exception("No compatible webUI version found")
    }

    private fun emitStatus(
        version: String,
        state: UpdateState,
        progress: Int,
        immediate: Boolean = false,
    ) {
        scope.launch {
            val status = getStatus(version, state, progress)

            if (immediate) {
                notifyFlow.emit(null)
                statusFlow.emit(status)
                return@launch
            }

            notifyFlow.emit(status)
        }
    }

    fun startDownloadInScope(
        flavor: WebUIFlavor,
        version: String,
    ) {
        @OptIn(DelicateCoroutinesApi::class)
        GlobalScope.launchIO {
            downloadVersion(flavor, version)
            serveWebUI()
        }
    }

    private suspend fun downloadVersion(
        flavor: WebUIFlavor,
        version: String,
    ) {
        emitStatus(version, DOWNLOADING, 0, immediate = true)

        try {
            val webUIZip = "${flavor.baseFileName}-$version.zip"
            val webUIZipPath = "$tmpDir/$webUIZip"
            val webUIZipURL = "${getDownloadUrlFor(flavor, version)}/$webUIZip"

            val log =
                KotlinLogging.logger("${logger.name} downloadVersion(version= $version, flavor= ${flavor.uiName})")
            log.info { "Downloading WebUI zip from the Internet..." }

            executeWithRetry(log, {
                downloadVersionZipFile(flavor, webUIZipURL, webUIZipPath) { progress ->
                    emitStatus(
                        version,
                        DOWNLOADING,
                        progress,
                    )
                }
            })
            File(applicationDirs.webUIRoot).deleteRecursively()

            // extract webUI zip
            log.info { "Extracting WebUI zip..." }
            extractDownload(webUIZipPath, applicationDirs.webUIRoot)
            log.info { "Extracting WebUI zip Done." }

            updateServedWebUIInfo(flavor)

            emitStatus(version, FINISHED, 100, immediate = true)
        } catch (e: Exception) {
            emitStatus(version, ERROR, 0, immediate = true)
            throw e
        }
    }

    private suspend fun downloadVersionZipFile(
        flavor: WebUIFlavor,
        url: String,
        filePath: String,
        updateProgress: (progress: Int) -> Unit,
    ) {
        val zipFile = File(filePath)
        zipFile.delete()

        val data = ByteArray(1024)

        zipFile.outputStream().use { webUIZipFileOut ->

            val connection = URI.create(url).toURL().openConnection() as HttpURLConnection
            connection.connect()
            val contentLength = connection.contentLength

            connection.inputStream.buffered().use { inp ->
                var totalCount = 0

                print("downloadVersionZipFile(${flavor.uiName}): Download progress: % 00")
                while (true) {
                    val count = inp.read(data, 0, 1024)

                    if (count == -1) {
                        break
                    }

                    totalCount += count
                    val percentage = (totalCount.toFloat() / contentLength * 100).toInt()
                    val percentageStr = percentage.toString().padStart(2, '0')
                    print("\b\b$percentageStr")

                    webUIZipFileOut.write(data, 0, count)

                    updateProgress(percentage)
                }
                println()
                logger.info { "downloadVersionZipFile(${flavor.uiName}): Downloading WebUI Done." }
            }
        }

        if (!isDownloadValid(flavor, filePath)) {
            throw Exception("Download is invalid")
        }
    }

    private suspend fun isDownloadValid(
        flavor: WebUIFlavor,
        zipFilePath: String,
    ): Boolean {
        val tempUnzippedWebUIFolderPath = zipFilePath.replace(".zip", "")

        extractDownload(zipFilePath, tempUnzippedWebUIFolderPath)

        val isDownloadValid = isLocalWebUIValid(flavor, tempUnzippedWebUIFolderPath)

        File(tempUnzippedWebUIFolderPath).deleteRecursively()

        return isDownloadValid
    }

    private fun extractDownload(
        zipFilePath: String,
        targetPath: String,
    ) {
        File(targetPath).mkdirs()
        ZipFile(zipFilePath).use { it.extractAll(targetPath) }
    }

    suspend fun isUpdateAvailable(
        flavor: WebUIFlavor,
        currentVersion: String = getLocalVersion(),
        raiseError: Boolean = false,
    ): Pair<String, Boolean> =
        try {
            val isServedWebUIForCurrentFlavor = flavor.uiName == getServedWebUIFlavor().uiName
            val latestCompatibleVersion = getLatestCompatibleVersion(flavor)
            val isVersionUpdateAvailable = latestCompatibleVersion != currentVersion

            val isUpdateAvailable = !isServedWebUIForCurrentFlavor || isVersionUpdateAvailable
            Pair(latestCompatibleVersion, isUpdateAvailable)
        } catch (e: Exception) {
            logger.warn(e) { "isUpdateAvailable: check failed due to" }

            if (raiseError) {
                throw e
            }

            Pair("", false)
        }
}
