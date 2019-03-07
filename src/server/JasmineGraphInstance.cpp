/**
Copyright 2019 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

#include "JasmineGraphInstance.h"
#include "../util/logger/Logger.h"

#include <iostream>
#include <unistd.h>

Logger worker_logger;

void JasmineGraphInstance::start_running(int serverPort, int serverDataPort) {
    worker_logger.log("Worker started", "info");
    worker_logger.log("Running the server...", "info");

    this->sqlite = *new SQLiteDBInterface();
    this->sqlite.init();


}

bool JasmineGraphInstance::isRunning() {
    return true;
}