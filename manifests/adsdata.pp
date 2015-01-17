# Some const. variables
$path_var = "/usr/bin:/usr/sbin:/bin:/usr/local/sbin:/usr/sbin:/sbin"
$build_packages = ['firefox', 'python', 'python-pip', 'python-dev', 'libpq-dev', 'libxml2-dev', 'libxslt1-dev', 'mongodb-org', 'jython', 'graphviz']
$pip_requirements = "requirements.txt"

# Latest mongoDB
exec {'add_repo':
	 command => 'sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10 && 
	 echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | sudo tee /etc/apt/sources.list.d/mongodb.list',
	 path => $path_var,
}

# Update package list
exec {'apt_update_1':
	command => 'apt-get update && touch /etc/.apt-updated-by-puppet1',
	creates => '/etc/.apt-updated-by-puppet1',
	path => $path_var,
}

# Install packages
package {$build_packages:
	ensure => installed,
	require => Exec['apt_update_1'],
}

# Install all python dependencies for selenium and general software
exec {'pip_install_modules':
	command => "pip install -r ${pip_requirements}",
	logoutput => on_failure,
	path => $path_var,
	tries => 2,
	timeout => 1000, # This is only require for Scipy/Matplotlib - they take a while
	require => Package[$build_packages],
}

Exec['add_repo'] -> Exec['apt_update_1'] -> Package[$build_packages] -> Exec['pip_install_modules']