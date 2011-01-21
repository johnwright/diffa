#
# Copyright (C) 2010-2011 LShift Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'rubygems'
require 'net/github-upload'

build_number = ARGV[0]
login = ARGV[1]
token = ARGV[2]

repos = 'lshift/diffa'

zip_package = Dir['dist/**/*.zip'].shift
war_package = Dir['agent/**/*.war'].shift

gh = Net::GitHub::Upload.new(
  :login => login,
  :token => token
)

zip_link = gh.replace(
  :repos => repos,
  :file  => zip_package,
  :description => "Standalone Diffa agent - build ##{build_number}"
)

puts "Succesfully uploaded #{zip_package} build number #{build_number} to #{zip_link}" 

war_link = gh.replace(
  :repos => repos,
  :file  => war_package,
  :description => "Diffa war archive - build ##{build_number}"
)

puts "Succesfully uploaded #{war_package} build number #{build_number} to #{war_link}"
