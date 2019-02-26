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
      mention: 'mention' },
    node_read:       '',
    mention_read:    '',

    node_alias:      true,
    mention_alias:   true,

    connect: {
      host: 'localhost:9202',
      log: true },
    another_methods: [:bulk, :update, :index],
    verboom_bulk: true } }

def test_count
  klass.mention.count(
    query: {
      bool: {
        filter: {
          term: {
            resource: 'resource' } } } })
end

def create_ids(ids, data = [])
  [ids].flatten.each_with_index do |id, index|
    es.index(
      index: klass.mention.config[:idx_read],
      type:  klass.mention.config[:type],
      body: { content: data[index] || 'data', resource: 'resource' },
      id:   id)
  end
  es.indices.refresh(index: klass.mention.config[:idx_read])
end

def delete_ids(ids)
  [ids].flatten.each do |id|
    es.delete(
      index: klass.mention.config[:idx_read],
      type:  klass.mention.config[:type],
      id:    id)
  end
end
