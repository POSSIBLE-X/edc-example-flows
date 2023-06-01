/*
 *  Copyright (c) 2022 Fraunhofer Institute for Software and Systems Engineering
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Fraunhofer Institute for Software and Systems Engineering - initial API and implementation
 *
 */

rootProject.name = "samples"

pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        mavenLocal()
    }
}

// transfer
include("transfer:transfer-06-consumer-pull-http:http-pull-connector")
include("transfer:transfer-06-consumer-pull-http:consumer-pull-backend-service")

include("transfer:transfer-07-provider-push-http:http-push-connector")
include("transfer:transfer-07-provider-push-http:provider-push-http-backend-service")

// modules for code samples ------------------------------------------------------------------------
include(":other:custom-runtime")
