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

    node_write_date: true,
    node_read_date: true,
    mention_write_date: true,
    mention_read_date: true,
    node_max_age: 2,
    mention_max_age: 2,

    languages_write: %w[cs sk],

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

def create_ids(es, ids, data = [])
  es.mention.save_document(docs: create_bulk(ids, data))
  es.mention.refresh
end

def create_bulk(ids, data)
  ids.each_with_index.map do |id, index|
    temp = { '_id' => id }
    temp['content'] = (data[index] || index).to_s if data
    temp
  end
end

def delete_ids(es, ids)
  es.mention.delete_document(docs: create_bulk(ids, nil))
  es.mention.refresh
end
