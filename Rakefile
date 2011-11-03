task :prepare_versions do
  fail('VERSION must be provided - eg, VERSION=0.9.3') unless ENV['VERSION']

  @version = ENV['VERSION']
  @tag_name = "accent_#{@version.gsub(/\./, "_")}"
end

task :prepare_dev_version do
  @dev_version = ENV['DEV_VERSION']
  unless @dev_version
    require 'rexml/document'

    pom = REXML::Document.new(File.read('pom.xml'))
    @dev_version = pom.elements["project/version"].text
  end
end

task :prepare => [:prepare_versions, :prepare_dev_version] do
  puts "Preparing release #{@version} with tag #{@tag_name}, next dev version will be #{@dev_version}"
  sh "mvn release:clean release:prepare -B -DpushChanges=true -DpreparationGoals=validate -DreleaseVersion=#{@version} -DdevelopmentVersion=#{@dev_version} -Dtag=#{@tag_name}"
end

task :release => :prepare do
  puts "Performing release #{@version}"

  sh("rm -rf checkout")
  sh("mkdir -p checkout")

  chdir('checkout') do
    sh "git clone git@github.com:lshift/diffa.git"
    chdir('diffa') do
      sh "git checkout #{@tag_name}"
      sh "mvn clean install -Dmaven.test.skip=true -Djetty.skip=true"

      puts "Deploying to sonatype"
      chdir('participant-support') do
        sh %Q{mvn -Dgpg.passphrase="xxxxxxx" -Darguments="-Dgpg.passphrase=xxxxxxxxx" -Dmaven.test.skip=true -Djetty.skip=true deploy}
      end

      puts "Deploying release war to s3"
      chdir('agent') do
        sh "mvn deploy -Dmaven.test.skip=true -Djetty.skip=true"
      end

      puts "Deploying release zip to s3"
      chdir('dist') do
        sh "s3cmd put target/diffa-*.zip s3://diffa-packages/releases"
      end

    end
  end

end
