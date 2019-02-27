module ElasticsearchRasi
  module TimeIndexName
    refine Time do
      def index_name_date
        strftime('%Y%m')
      end
    end
  end
end
