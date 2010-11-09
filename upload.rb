require 'rubygems'
require 'net/github-upload'

build_number = ARGV[0]
login = ARGV[1]
token = ARGV[2]

repos = 'lshift/diffa'

zip_package = Dir['*.zip'].shift
war_package = Dir['*.war'].shift

gh = Net::GitHub::Upload.new(
  :login => login,
  :token => token
)

zip_link = gh.upload(
  :repos => repos,
  :file  => zip_package,
  :description => "Standalone Diffa agent - build ##{build_number}"
)

puts "Succesfully uploaded #{zip_package} build number #{build_number} to #{zip_link}" 

war_link = gh.upload(
  :repos => repos,
  :file  => war_package,
  :description => "Diffa war archive - build ##{build_number}"
)

puts "Succesfully uploaded #{war_package} build number #{build_number} to #{war_link}"
