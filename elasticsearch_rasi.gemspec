# encoding:utf-8
Gem::Specification.new do |s|
  s.name        = 'elasticsearch_rasi'
  s.version     = '0.2.0'
  s.date        = '2015-02-09'
  s.summary     = "ElasticSearch for Rasi"
  s.description = "Post and mentions elasticsearch"
  s.authors     = ["Tomas Hrabal"]
  s.email       = 'hrabal.tomas@gmail.com'
  s.files       = `git ls-files`.split("\n")
  s.require_paths = ['lib']
  s.test_files  = s.files.grep(%r{^(test|spec|features)/})
  s.homepage    =
    'http://rubygems.org/gems/hola'
  s.license     = 'MIT'

  s.add_dependency 'elasticsearch', '~> 5.0', '>= 5.0.4'
  s.add_dependency 'typhoeus', '~> 0.7'
  s.add_dependency 'multi_json', '~> 1.11', '>= 1.11.2'
  s.add_dependency 'activesupport', '~> 4.2', '>= 4.2.7'
  s.add_dependency 'unicode_utils', '~> 1.4'
  s.add_development_dependency "test-unit"
  s.add_development_dependency "rake"
  s.add_development_dependency "rspec"
  s.add_development_dependency 'webmock', '~> 3.5', '>= 3.5.1'
  s.add_development_dependency "pry"
end
