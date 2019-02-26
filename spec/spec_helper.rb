# encoding:utf-8
require 'rspec'
require 'pry'
$LOAD_PATH.unshift(File.expand_path(File.join(__dir__, '..', 'lib')))
require 'elasticsearch_rasi'
ES = {
  disputatio: {
    base: 'test',
    node_suffix: '_articles',
    mention_suffix: '_mentions',
    file: {
      node: 'node',
      mention: 'mention'
    },
    node_read:       '',
    mention_read:    '',

    node_alias:      true,
    mention_alias:   true,

    connect: {
      host: 'localhost:9202',
      log: true },
    another_methods: [:bulk, :update, :index],
    verboom_bulk: true }
}
