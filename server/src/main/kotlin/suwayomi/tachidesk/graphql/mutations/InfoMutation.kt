@file:Suppress("RedundantNullableReturnType", "unused")

package suwayomi.tachidesk.graphql.mutations

import io.github.oshai.kotlinlogging.KotlinLogging
import kotlin.concurrent.thread
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import suwayomi.tachidesk.graphql.directives.RequireAuth
import suwayomi.tachidesk.graphql.types.UpdateState.DOWNLOADING
import suwayomi.tachidesk.graphql.types.UpdateState.ERROR
import suwayomi.tachidesk.graphql.types.UpdateState.IDLE
import suwayomi.tachidesk.graphql.types.WebUIFlavor
import suwayomi.tachidesk.graphql.types.WebUIUpdateStatus
import suwayomi.tachidesk.server.JavalinSetup.future
import suwayomi.tachidesk.server.util.ExitCode
import suwayomi.tachidesk.server.util.WebInterfaceManager
import suwayomi.tachidesk.server.util.shutdownApp
import java.util.concurrent.CompletableFuture
import kotlin.time.Duration.Companion.seconds

class InfoMutation {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    data class WebUIUpdateInput(
        val clientMutationId: String? = null,
    )

    data class WebUIUpdatePayload(
        val clientMutationId: String?,
        val updateStatus: WebUIUpdateStatus,
    )

    @RequireAuth
    fun updateWebUI(input: WebUIUpdateInput): CompletableFuture<WebUIUpdatePayload?> {
        return future {
            withTimeout(30.seconds) {
                if (WebInterfaceManager.status.value.state === DOWNLOADING) {
                    return@withTimeout WebUIUpdatePayload(input.clientMutationId, WebInterfaceManager.status.value)
                }

                val flavor = WebUIFlavor.current

                val (version, updateAvailable) = WebInterfaceManager.isUpdateAvailable(flavor)

                if (!updateAvailable) {
                    val didUpdateCheckFail = version.isEmpty()

                    return@withTimeout WebUIUpdatePayload(
                        input.clientMutationId,
                        WebInterfaceManager.getStatus(version, if (didUpdateCheckFail) ERROR else IDLE),
                    )
                }
                try {
                    WebInterfaceManager.startDownloadInScope(flavor, version)
                } catch (e: Exception) {
                    // ignore since we use the status anyway
                }

                WebUIUpdatePayload(
                    input.clientMutationId,
                    updateStatus = WebInterfaceManager.status.first { it.state == DOWNLOADING },
                )
            }
        }
    }

    @RequireAuth
    fun resetWebUIUpdateStatus(): CompletableFuture<WebUIUpdateStatus?> =
        future {
            withTimeout(30.seconds) {
                val isUpdateFinished = WebInterfaceManager.status.value.state != DOWNLOADING
                if (!isUpdateFinished) {
                    throw Exception("Status reset is not allowed during status \"$DOWNLOADING\"")
                }

                WebInterfaceManager.resetStatus()

                WebInterfaceManager.status.first { it.state == IDLE }
            }
        }

    @RequireAuth
    fun shutdownServer(): CompletableFuture<String?> =
        future {
            thread { shutdownApp(ExitCode.Success) }
            "shutting_down"
        }

    @RequireAuth
    fun restartServer(): CompletableFuture<String?> =
        future {
            thread {
                try {
                    val jarUri = InfoMutation::class.java.protectionDomain.codeSource.location.toURI()
                    var jarPath = jarUri.path
                    if (System.getProperty("os.name").startsWith("Windows") && jarPath.startsWith("/")) {
                        jarPath = jarPath.removePrefix("/")
                    }
                    val javaBinName = if (System.getProperty("os.name").startsWith("Windows")) "javaw.exe" else "java"
                    val javaBin = "${System.getProperty("java.home")}/bin/$javaBinName"
                    ProcessBuilder(javaBin, "-jar", jarPath)
                        .inheritIO()
                        .start()
                } catch (e: Exception) {
                    logger.error(e) { "Failed to restart server" }
                }
                shutdownApp(ExitCode.Success)
            }
            "restarting"
        }
}
