require 'rubygems'
require 'net/github-upload'

build_number = ARGV[0]
login = ARGV[1]
token = ARGV[2]

repos = 'lshift/diffa'

zip_package = Dir['*.zip'].shift

gh = Net::GitHub::Upload.new(
  :login => login,
  :token => token
)

direct_link = gh.replace(
  :repos => repos,
  :file  => zip_package,
  :description => "Standalone Diffa Agent - build ##{build_number}"
)

puts "Succesfully uploaded #{zip_package} build number #{build_number} to #{direct_link}" 
