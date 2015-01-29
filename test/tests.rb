require 'elasticsearch_rasi'
require "test/unit"

class ElasticSearchTest < Test::Unit::TestCase

  def test_initialize_without_index
    assert_equal nil, ElasticSearch.new(nil).idx
  end

  def test_initialize
    assert_equal nil, ElasticSearch.new('foo', {:direct_idx => false, :logging => false}).idx
  end

  def test_initialize_true
    assert_equal 'foo', ElasticSearch.new('foo', {:direct_idx => true, :logging => false}).idx
  end

  def test_initialize_with_constant
    $ES = {
      :foo            => 'foo',
      :foo_mentions   => 'foo_mentions',
      :mentions_sufix => '_mentions'
    }
    assert_equal 'foo', ElasticSearch.new('foo', {:logging => false}).idx
    assert_equal 'foo_mentions', ElasticSearch.new('foo', {:logging => false}).idx_mentions
  end

end
