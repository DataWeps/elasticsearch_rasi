# encoding:utf-8
require 'rspec'
require 'pry'
$LOAD_PATH.unshift(File.expand_path(File.join(__dir__, '..', 'lib')))
require 'elasticsearch_rasi'
ES = {
    twitter: {
      base: 'twitter',
      file: {
        node: 'twitter_users',
        mention: 'twitter_mentions' },
      node_suffix: '_users',
      mention_suffix: '_mentions',

      node_read: '',
      node_write: '_current',

      mention_read: '',
      mention_write: '',
      mention_write_date: true,
      mention_write_date_base: 'twitter_mentions',
      mention_read_date_base: 'twitter_mentions',
      mention_read_date: true,

      node_alias: false,
      mention_alias: true,

      rotation: {
        mention:{
          rotate: true
        }
      },

      connect: {
        reload_on_failure: true,
        host: ['localhost:9202'],
        log: true
      },
      connect_attempts: 2,
      connect_sleep: 2
  },
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
      host: 'localhost:9202',
      log: true },
      # { connect: {
      #   host: 'localhost:9203',
      #   log: false } }
    # ],
    another_methods: [:bulk, :update, :index],
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
      host: 'localhost:9202',
      log: false }
    # connect_another: [
    #   { host: 'es1.weps.cz:9200', log: true } ],
    # another_methods: [:bulk, :update, :index]
  },
  connect_sleep: 5,
  connect_attempts: 2,
}
