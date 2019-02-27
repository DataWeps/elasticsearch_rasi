# encoding:utf-8
require 'active_support/core_ext/object/deep_dup'
require 'active_support/core_ext/object/blank'

require 'utils/refines/time_index_name'

module ElasticsearchRasi
  module Request
    using TimeIndexName
    CONTENT_TYPE     = { content_type: 'application/json' }.freeze
    CONNECT_ATTEMPTS = 5
    CONNECT_SLEEP    = 0.1

    def request(method, params)
      send_request(method, params)
    end

    def clear_request(method, params)
      response = send_request(method, params)
      return false if response.include?(:error)
      response
    end

  private

    def another_es?(method)
      !@es_another.empty? &&
        !@config[:another_methods].blank? && @config[:another_methods].include?(method.to_sym)
    end

    def change_type?(config)
      config.include?("#{@rasi_type}_type".to_sym)
    end

    def change_index?(config)
      config.include?("#{@rasi_type}_index".to_sym) ||
        config["#{@rasi_type}_write_date".to_sym]
    end

    def change_index(key, config, params)
      return params[key] if
        params.include?(key) && !change_index?(config)
      if config.include?("#{@rasi_type}_index".to_sym)
        config["#{@rasi_type}_index".to_sym]
      elsif config.include?("#{@rasi_type}_write_date".to_sym)
        "#{config["#{@rasi_type}_write_date_base".to_sym]}_#{Time.now.index_name_date}"
      else
        params[key]
      end
    end

    def prepare_params!(config, params)
      params[:body].each do |in_data|
        data = in_data.values[0]
        data[:_type] = config["#{@rasi_type}_type".to_sym] if
          data.include?(:_type) && change_type?(config)
        data[:_index] = change_index(:_index, config, data)
      end
    end

    def send_request(method, params)
      method = method.to_sym
      counter = 0
      begin
        response = @es.send(method, params)
        # Strange behavior, sometimes ES gem returns empty result, but with OK headers
        raise(Faraday::ConnectionFailed, 'Blank response') if response.blank?
        return response if
          !another_es?(method) || !%i[bulk index update].include?(method)
        @es_another.each do |es|
          next if es[:config].include?("save_#{@rasi_type}".to_sym) &&
                  es[:config]["save_#{@rasi_type}".to_sym] == false
          clone_params = params.deep_dup
          prepare_params!(es[:config], clone_params)
          es[:es].send(method.to_sym, clone_params)
        end
        response
      rescue Elasticsearch::Transport::Transport::Errors::InternalServerError => e
        { 'errors' => e.message }
      rescue Elasticsearch::Transport::Transport::Errors::BadRequest => e
        { 'errors' => e.message }
      rescue Faraday::ConnectionFailed, Faraday::TimeoutError,
             Elasticsearch::Transport::Transport::Errors::ServiceUnavailable => e
        counter += 1
        return { 'errors' => e.message } if counter > (@config[:connect_attempts] || CONNECT_ATTEMPTS)
        sleep(@config[:connect_sleep] || CONNECT_SLEEP)
        retry
      end
    end
  end
end
