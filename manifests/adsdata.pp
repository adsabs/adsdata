# Some const. variables
$path_var = "/usr/bin:/usr/sbin:/bin:/usr/local/sbin:/usr/sbin:/sbin"
$build_packages = ['firefox', 'python', 'python-pip', 'python-dev']

exec {'apt_update_1':
  command => 'apt-get update && touch /etc/.apt-updated-by-puppet1',
  creates => '/etc/.apt-updated-by-puppet1',
  path => $path_var,
}

package {$build_packages:
  ensure => installed,
  require => Exec['apt_update_1'],
}

Exec['apt_update_1'] -> Package[$build_packages]