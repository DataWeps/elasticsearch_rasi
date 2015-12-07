# encoding:utf-8
require 'rspec'
$LOAD_PATH.unshift(File.expand_path(File.join(__dir__, '..', 'lib')))
require 'elasticsearch_rasi'
ES = {
  :disputatio => {
    :base           => 'disputatio',
    :node_suffix    => '_articles',
    :mention_suffix => '_mentions',

    :node_read      => '',
    :node_write     => '_current',

    :mention_read   => '',
    :mention_write  => '_current',

    :node_client    => 'news',
    :mention_client => 'discussions',

    :node_alias     => true,
    :mention_alias  => true,
    connect: {
      host: 'localhost:9200',
      log:  true
    }
  },
  connect_sleep: 5,
  connect_attempts: 2
}
