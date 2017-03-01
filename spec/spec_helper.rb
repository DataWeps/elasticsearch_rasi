# encoding:utf-8
require 'rspec'
require 'pry'
$LOAD_PATH.unshift(File.expand_path(File.join(__dir__, '..', 'lib')))
require 'elasticsearch_rasi'
ES = {
  :disputatio => {
    :base           => 'disputatio',
    :node_suffix    => '_articles',
    :mention_suffix => '_mentions',
    file: {
      node: 'node',
      mention: 'mention'
    },

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
      log:  true },
    # connect_another: [
    #   { host: 'localhost:9997', log: true } ],
    another_methods: [:bulk, :update],
    verboom_bulk: true
  },
  forums:  {
    base: 'forums',
    node_suffix: '_topics',
    mention_suffix: '_mentions',
    file: {
      node: 'node',
      mention: 'mention'
    },
    node_read:  '',
    node_write:  '',

    mention_read:  '',
    mention_write: '_current',

    node_client:  '',
    mention_client: 'forums',

    node_alias:  false,
    mention_alias:  true,
    connect: {
      host: 'localhost:9200',
      log:  true }
    # connect_another: [
    #   { host: 'es1.weps.cz:9200', log: true } ],
    # another_methods: [:bulk, :update, :index]
  },
  connect_sleep: 5,
  connect_attempts: 2,
}
