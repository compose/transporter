# -*- mode: ruby -*-
# vi: set ft=ruby :
system("
    if [ #{ARGV[0]} = 'up' ]; then
		ansible-galaxy install -r .ansible-requirements.yml --force
    fi
")

VAGRANT_VERSION = 2
Vagrant.configure(VAGRANT_VERSION) do |config|
	config.vm.provider "vmware_fusion" do |v|
		v.vmx["memsize"] = "2048"
	end
	config.vm.provider "virtualbox" do |v|
		v.memory = 2048
	end

	config.vm.define "transporter-demo" do |server|
		server.vm.box = "hashicorp/precise64"
		server.vm.hostname = "transporter-demo"
		server.vm.provision :ansible do |ansible|
			ansible.playbook = ".ansible-provision.yml"
		end
	end
end
