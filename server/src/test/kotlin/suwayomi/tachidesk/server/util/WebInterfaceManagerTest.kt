package suwayomi.tachidesk.server.util

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import org.junit.jupiter.api.Test
import java.io.File
import kotlin.test.assertContains
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class WebInterfaceManagerTest {
    /**
     * Reads the injected JS script block from WebInterfaceManager source
     * and verifies the key behavioral patterns that fix the SPA navigation bug.
     *
     * When navigating from a chapter reader back to the main page in the SPA,
     * the sidebar DOM is recreated and the injected buttons disappear.
     * The fix ensures the MutationObserver keeps running and re-injects
     * when the sidebar reappears.
     */
    private val injectedScript: String by lazy {
        val sourceDir = File("src/main/kotlin/suwayomi/tachidesk/server/util")
        val sourceFile = File(sourceDir, "WebInterfaceManager.kt")
        val content = sourceFile.readText()
        val scriptStart = content.indexOf("<script>")
        val scriptEnd = content.indexOf("</script>", scriptStart)
        content.substring(scriptStart + "<script>".length, scriptEnd)
    }

    @Test
    fun `injected flag is reset when About element is not found`() {
        assertContains(injectedScript, "if (!about) { injected = false;")
    }

    @Test
    fun `injected guard is checked after About element lookup`() {
        val aboutCheckIndex = injectedScript.indexOf("if (!about) { injected = false;")
        val injectedGuardIndex = injectedScript.indexOf("if (injected) return true;")
        assertTrue(aboutCheckIndex >= 0, "About element check should exist")
        assertTrue(injectedGuardIndex >= 0, "Injected guard should exist")
        assertTrue(aboutCheckIndex < injectedGuardIndex, "About check must come before injected guard")
    }

    @Test
    fun `mutation observer stays alive after initial injection`() {
        assertFalse(injectedScript.contains("obs.disconnect()"), "Observer should never disconnect")
    }

    @Test
    fun `injectSidebar is called unconditionally in observer callback`() {
        // The observer callback should call injectSidebar() directly,
        // not guarded by an if check
        val observerBody = injectedScript.substringAfter("new MutationObserver(function() {")
            .substringBefore("});")
        assertContains(observerBody, "injectSidebar();")
        assertFalse(observerBody.contains("if (injectSidebar())"), "Observer should not guard injectSidebar with if")
    }

    @Test
    fun `tryInject does not have early return`() {
        val tryInjectBody = injectedScript.substringAfter("function tryInject() {")
            .substringBefore("function tryInject")
            .substringBefore("if (document.body)")
        assertFalse(tryInjectBody.contains("if (injectSidebar()) return;"),
            "tryInject should not early-return after initial injection attempt")
    }
}
