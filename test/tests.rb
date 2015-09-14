require 'elasticsearch_rasi'
require "test/unit"

class ElasticSearchTest < Test::Unit::TestCase

  def test_initialize_without_index
    assert_raise(ArgumentError) {
     ElasticSearchRasi.new(:foo)
    }
  end

  def test_initialize
    assert_raise(ArgumentError) {
      ElasticSearchRasi.new('foo', {:direct_idx => false, :logging => false})
    }
  end

  def test_initialize_true
    assert_equal 'foo', ElasticSearchRasi.new(
      'foo', {:direct_idx => true, :logging => false}
    ).idx
  end

  def test_initialize_with_constant
    $ES = {
      :foo => {
        :base           => 'foo',
        :node_suffix    => '_node',
        :mention_suffix => '_mentions'
      }
    }
    assert_equal 'foo_node', ElasticSearchRasi.new(:foo).idx_node_read
    assert_equal 'foo_mentions', ElasticSearchRasi.new(:foo).idx_mention_read
    $ES = nil
  end

  def test_get_document
    @es = ElasticSearchRasi.new :rss_feeds, { direct_idx: true }
    @es.get_document 'http%3A%2F%2Fkrkonossky.denik.cz%2Frss%2Fzpravy_region.htm'
  end

end
