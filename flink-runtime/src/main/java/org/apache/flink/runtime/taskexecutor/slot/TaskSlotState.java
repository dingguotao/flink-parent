/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor.slot;

/** Internal task slot state */
enum TaskSlotState {
    // clouding 注释: 2022/3/26 13:23
    //         ACTIVE表示TaskSlot已经提供给某个JobMaster使用了
    ACTIVE, // Slot is in active use by a job manager responsible for a job
    // clouding 注释: 2022/3/26 13:23
    //          ALLOCATED 表示已分配,但是还未成功提供给JobMaster.对来自ResourceManager的请求,
    //          首先会将TaskSlot标记为ALLOCATED状态,如果后续分配成功就修改为ACTIVE
    ALLOCATED, // Slot has been allocated for a job but not yet given to a job manager
    // clouding 注释: 2022/3/26 13:24
    //          RELEASING表示slot已经释放,但是上还存在运行的任务
    RELEASING // Slot is not empty but tasks are failed. Upon removal of all tasks, it will be
    // released
}
