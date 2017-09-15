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

#include <vector>

#include <mesos/mesos.hpp>

#include <process/owned.hpp>
#include <process/gtest.hpp>

#include <stout/gtest.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>

#include "slave/containerizer/mesos/containerizer.hpp"

#include "tests/cluster.hpp"
#include "tests/environment.hpp"
#include "tests/mesos.hpp"

#include "tests/containerizer/docker_archive.hpp"

using process::Future;
using process::Owned;

using std::map;
using std::string;
using std::vector;

using testing::TestParamInfo;
using testing::Values;
using testing::WithParamInterface;

using mesos::internal::slave::Containerizer;
using mesos::internal::slave::state::SlaveState;
using mesos::internal::slave::Fetcher;
using mesos::internal::slave::MesosContainerizer;

using mesos::master::detector::MasterDetector;

using mesos::slave::ContainerTermination;

namespace mesos {
namespace internal {
namespace tests {

class VolumeMemoryIsolatorTest : public MesosTest {};

// This test verifies that a volume with an absolute container path is
// properly mounted in the container's mount namespace.
TEST_F(VolumeMemoryIsolatorTest, ROOT_VolumeFromHost)
{
  string registry = path::join(sandbox.get(), "registry");
  AWAIT_READY(DockerArchive::create(registry, "test_image"));

  slave::Flags flags = CreateSlaveFlags();
  flags.isolation = "filesystem/linux,docker/runtime";
  flags.docker_registry = registry;
  flags.docker_store_dir = path::join(sandbox.get(), "store");
  flags.image_providers = "docker";

  Fetcher fetcher(flags);

  Try<MesosContainerizer*> create =
    MesosContainerizer::create(flags, true, &fetcher);

  ASSERT_SOME(create);

  Owned<Containerizer> containerizer(create.get());

  SlaveState state;
  state.id = SlaveID();

  AWAIT_READY(containerizer->recover(state));

  ContainerID containerId;
  containerId.set_value(UUID::random().toString());

  ContainerInfo containerInfo;
  containerInfo.set_type(ContainerInfo::MESOS);

  Volume* volume = containerInfo.add_volumes();
  volume->set_mode(Volume::RW);
  volume->set_container_path("/abs/ramdrive");

  Volume::Source* source = volume->mutable_source();
  source->set_type(Volume::Source::MEMORY_VOLUME);
  Volume::Source::MemoryVolume* memoryVolume = source->mutable_memory_volume();
  memoryVolume->set_size(32);

  CommandInfo command = createCommandInfo(
    "mount | grep -e \"^tmpfs on /abs/ramdrive type tmpfs.*size=32768k.*$\"");

  ExecutorInfo executor = createExecutorInfo("test_executor", command);

  string directory = path::join(flags.work_dir, "sandbox");
  ASSERT_SOME(os::mkdir(directory));

  Future<bool> launch = containerizer->launch(
    containerId,
    createContainerConfig(None(), executor, directory),
    map<string, string>(),
    None());

  AWAIT_ASSERT_TRUE(launch);

  Future<Option<ContainerTermination>> wait = containerizer->wait(containerId);

  AWAIT_READY(wait);
  ASSERT_SOME(wait.get());
  ASSERT_TRUE(wait->get().has_status());
  EXPECT_WEXITSTATUS_EQ(124, wait->get().status());
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
