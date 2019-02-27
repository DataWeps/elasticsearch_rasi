require 'multi_json'

module ElasticsearchRasi
  class JsonHelper
    class << self
      def load(data)
        MultiJson.load(data)
      end

      def dump(data)
        MultiJson.dump(data)
      end
    end
  end
end
