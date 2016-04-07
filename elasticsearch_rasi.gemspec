# encoding:utf-8

Gem::Specification.new do |s|
  s.name        = 'elasticsearch_rasi'
  s.version     = '0.2.0'
  s.date        = '2015-02-09'
  s.summary     = "ElasticSearch for Rasi"
  s.description = "Post and mentions elasticsearch"
  s.authors     = ["Tomas Hrabal"]
  s.email       = 'hrabal.tomas@gmail.com'
  s.files       = [
    "lib/elasticsearch_rasi.rb",
    "lib/elasticsearch_rasi/util.rb",
    "lib/elasticsearch_rasi/query.rb",
    "lib/elasticsearch_rasi/request.rb",
    "lib/elasticsearch_rasi/scroll.rb",
    "lib/elasticsearch_rasi/node.rb",
    "lib/elasticsearch_rasi/mention.rb",
    "lib/elasticsearch_rasi/rotation.rb",
  ]
  s.test_files    = s.files.grep(%r{^(test|spec|features)/})
  s.homepage    =
    'http://rubygems.org/gems/hola'
  s.license       = 'MIT'

  s.add_dependency 'curburger'
  s.add_dependency 'oj', '~> 2.11'
  s.add_development_dependency "test-unit"
end