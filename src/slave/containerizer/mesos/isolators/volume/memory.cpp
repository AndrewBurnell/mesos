// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "slave/containerizer/mesos/isolators/volume/memory.hpp"

#include <list>
#include <string>

#include <process/collect.hpp>
#include <process/id.hpp>
#include <process/owned.hpp>

#include <stout/foreach.hpp>
#include <stout/stringify.hpp>
#include <stout/strings.hpp>

#include <stout/os/mkdir.hpp>
#include <stout/os/write.hpp>

#ifdef __linux__
#include "linux/ns.hpp"
#endif // __linux__

#include "common/validation.hpp"

using std::list;
using std::string;

using process::Failure;
using process::Future;
using process::Owned;

using mesos::slave::ContainerClass;
using mesos::slave::ContainerConfig;
using mesos::slave::ContainerLaunchInfo;
using mesos::slave::ContainerState;
using mesos::slave::Isolator;

namespace mesos {
namespace internal {
namespace slave {


Try<Isolator*> VolumeMemoryIsolatorProcess::create(const Flags& flags)
{
  if (flags.launcher != "linux" ||
      !strings::contains(flags.isolation, "filesystem/linux")) {
    return Error("Memory volume isolation requires filesystem/linux isolator.");
  }

  Owned<MesosIsolatorProcess> process(new VolumeMemoryIsolatorProcess(flags));

  return new MesosIsolator(process);
}


VolumeMemoryIsolatorProcess::VolumeMemoryIsolatorProcess(const Flags& _flags)
  : ProcessBase(process::ID::generate("volume-memory-isolator")),
    flags(_flags) {}


bool VolumeMemoryIsolatorProcess::supportsNesting()
{
  // TODO(aburnell): nesting support?
  return false;
}


Future<Option<ContainerLaunchInfo>> VolumeMemoryIsolatorProcess::prepare(
    const ContainerID& containerId,
    const ContainerConfig& containerConfig)
{
  if (!containerConfig.has_container_info()) {
    return None();
  }

  const ContainerInfo& containerInfo = containerConfig.container_info();

  if (containerInfo.type() != ContainerInfo::MESOS) {
    return Failure(
        "Can only prepare the memory volume isolator for a MESOS container");
  }

  if (containerConfig.has_container_class() &&
      containerConfig.container_class() == ContainerClass::DEBUG) {
    return None();
  }

  ContainerLaunchInfo launchInfo;

  foreach (const Volume& volume, containerInfo.volumes()) {
      // TODO(aburnell) validation?
    if (!volume.has_source() ||
        !volume.source().has_type() ||
        volume.source().type() != Volume::Source::MEMORY_VOLUME) {
      continue;
    }

    if (!volume.source().has_memory_volume()) {
      return Failure("volume.source.memory_volume is not specified");
    }

    string targetContainerPath;
    string hostMountPoint = "";
    const bool isPathAbsolute = path::absolute(volume.container_path());
    const bool hasRootFS = containerConfig.has_rootfs();
    // Determine where to create mount points and where to mount tmpfs
    if (isPathAbsolute && hasRootFS) {
      targetContainerPath = path::join(
          containerConfig.rootfs(),
          volume.container_path());
      hostMountPoint = targetContainerPath;
    } else if (isPathAbsolute && !hasRootFS) {
      targetContainerPath = volume.container_path();
      if (!os::exists(targetContainerPath)) {
        return Failure(
          "Absolute container path '" + targetContainerPath + "' "
          "does not exist");
      }
    } else if (!isPathAbsolute && hasRootFS) {
      targetContainerPath = path::join(
        containerConfig.rootfs(),
        flags.sandbox_directory,
        volume.container_path());
      hostMountPoint = path::join(
        containerConfig.directory(),
        volume.container_path());
    } else if (!isPathAbsolute && !hasRootFS) {
      targetContainerPath = path::join(
        containerConfig.rootfs(),
        volume.container_path());
      hostMountPoint = path::join(
        containerConfig.directory(),
        volume.container_path());
    }
    // Create mount points if necessary
    if (hostMountPoint != "") {
      Try<Nothing> mkdir = os::mkdir(hostMountPoint);
      LOG(INFO) << "Creating memory volume mount point at '"
                << hostMountPoint << "'";
      if (mkdir.isError()) {
        return Failure(
            "Failed to create directory '" +
            Path(hostMountPoint).dirname() + "' "
            "for the target mount file: " + mkdir.error());
      }
    }

    LOG(INFO) << "Setting memory volume mount point to '"
              << targetContainerPath << "'";

    const string size = std::to_string(volume.source().memory_volume().size());
    // Mount tmpfs in the container.
    CommandInfo* command = launchInfo.add_pre_exec_commands();
    command->set_shell(false);
    command->set_value("mount");
    command->add_arguments("mount");
    command->add_arguments("-n");
    command->add_arguments("-t");
    command->add_arguments("tmpfs");
    command->add_arguments("-o");
    command->add_arguments("nodev,nosuid,noexec,size=" + size + "M");
    command->add_arguments("tmpfs");
    command->add_arguments(targetContainerPath);
  }

  return launchInfo;
}

} // namespace slave {
} // namespace internal {
} // namespace mesos {
