package suwayomi.tachidesk.global.controller

/*
 * Copyright (C) Contributors to the Suwayomi project
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import io.github.oshai.kotlinlogging.KotlinLogging
import io.javalin.http.HttpStatus
import kotlin.concurrent.thread
import suwayomi.tachidesk.server.JavalinSetup.Attribute
import suwayomi.tachidesk.server.JavalinSetup.getAttribute
import suwayomi.tachidesk.server.user.requireUser
import suwayomi.tachidesk.server.util.ExitCode
import suwayomi.tachidesk.server.util.handler
import suwayomi.tachidesk.server.util.shutdownApp
import suwayomi.tachidesk.server.util.withOperation

object ServerController {
    private val logger = KotlinLogging.logger {}

    val shutdown =
        handler(
            documentWith = {
                withOperation {
                    summary("Shutdown Suwayomi-Server")
                    description("Shuts down the server process")
                }
            },
            behaviorOf = { ctx ->
                ctx.getAttribute(Attribute.TachideskUser).requireUser()

                ctx.json(mapOf("status" to "shutting_down"))

                thread { shutdownApp(ExitCode.Success) }
            },
            withResults = {
                json<Map<String, String>>(HttpStatus.OK)
            },
        )

    val restart =
        handler(
            documentWith = {
                withOperation {
                    summary("Restart Suwayomi-Server")
                    description("Restarts the server process")
                }
            },
            behaviorOf = { ctx ->
                ctx.getAttribute(Attribute.TachideskUser).requireUser()

                ctx.json(mapOf("status" to "restarting"))

                thread {
                    try {
                        val jarUri = ServerController::class.java.protectionDomain.codeSource.location.toURI()
                        var jarPath = jarUri.path
                        if (System.getProperty("os.name").startsWith("Windows") && jarPath.startsWith("/")) {
                            jarPath = jarPath.removePrefix("/")
                        }
                        val javaBinName = if (System.getProperty("os.name").startsWith("Windows")) "javaw.exe" else "java"
                        val javaBin = "${System.getProperty("java.home")}/bin/$javaBinName"
                        logger.info { "Restarting with: $javaBin -jar $jarPath" }
                        ProcessBuilder(javaBin, "-jar", jarPath)
                            .inheritIO()
                            .start()
                    } catch (e: Exception) {
                        logger.error(e) { "Failed to restart server" }
                    }
                    shutdownApp(ExitCode.Success)
                }
            },
            withResults = {
                json<Map<String, String>>(HttpStatus.OK)
            },
        )
}
