/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.master.dispatch.host;

import org.apache.dolphinscheduler.common.enums.OS;
import org.apache.dolphinscheduler.common.enums.WorkerPlatform;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.apache.dolphinscheduler.server.master.dispatch.exceptions.WorkerGroupNotFoundException;
import org.apache.dolphinscheduler.server.master.dispatch.host.assign.HostWorker;
import org.apache.dolphinscheduler.server.master.registry.ServerNodeManager;

import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * common host manager
 */
public abstract class CommonHostManager implements HostManager {

    /**
     * server node manager
     */
    @Autowired
    protected ServerNodeManager serverNodeManager;

    @Override
    public Optional<Host> select(String workerGroup, Integer workerPlatform) throws WorkerGroupNotFoundException {
        List<HostWorker> candidates = getWorkerCandidates(workerGroup, workerPlatform);
        if (CollectionUtils.isEmpty(candidates)) {
            return Optional.empty();
        }
        return Optional.ofNullable(select(candidates));
    }

    protected abstract HostWorker select(Collection<HostWorker> nodes);


    /**
     * Retrieves a list of worker candidates based on the given worker group and worker platform.
     *
     * @param workerGroup    The name of the worker group.
     * @param workerPlatform The platform of the worker (optional). Use null or WorkerPlatform.ANY.getCode()
     *                       to retrieve candidates regardless of platform. Use WorkerPlatform.UNIX.getCode()
     *                       to retrieve only Unix candidates, and use WorkerPlatform.WINDOWS.getCode()
     *                       to retrieve only Windows candidates.
     * @return A list of HostWorker objects representing the worker candidates.
     * @throws WorkerGroupNotFoundException if the worker group is not found.
     */
    protected List<HostWorker> getWorkerCandidates(String workerGroup, Integer workerPlatform) throws WorkerGroupNotFoundException {
        List<HostWorker> hostWorkers = new ArrayList<>();
        Set<String> nodes = serverNodeManager.getWorkerGroupNodes(workerGroup);
        if (CollectionUtils.isNotEmpty(nodes)) {
            for (String node : nodes) {
                serverNodeManager.getWorkerNodeInfo(node)
                        .filter(workerNodeInfo -> {
                            if (workerPlatform == null || workerPlatform == WorkerPlatform.ANY.getCode()) {
                                return true;
                            }
                            OS osType = OSUtils.getOSType(workerNodeInfo.getOsName());
                            return (osType == OS.UNIX && workerPlatform == WorkerPlatform.UNIX.getCode())
                                    || (osType == OS.WINDOWS && workerPlatform == WorkerPlatform.WINDOWS.getCode());
                        })
                        .ifPresent(workerNodeInfo -> hostWorkers.add(HostWorker.of(node, workerNodeInfo.getWorkerHostWeight(), workerGroup)));
            }
        }
        return hostWorkers;
    }
}
