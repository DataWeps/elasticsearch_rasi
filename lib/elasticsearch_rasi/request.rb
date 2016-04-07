# encoding:utf-8

class ElasticSearchRasi

  module Request

    CONTENT_TYPE = {:content_type => 'application/json'}

  private

    def request_elastic(method, url, data = nil, params = {})
      req_params = []
      _url = "#{@url}/#{url}"
      req_params.push(method, _url)
      req_params << data   if data
      req_params << params if params && !params.empty?

      content_type_position = method == :get ? 2 : 4
      if req_params[content_type_position].nil?
        req_params << CONTENT_TYPE.dup
      elsif !req_params[-1].include?(:content_type)
        req_params[-1] = req_params[-1].merge(CONTENT_TYPE)
      end

      response = nil
      response = attempt_elastic req_params

      unless response && !response[:error]
        return false if method == :get && response[:error] == "404 Not Found"
        return false
      end

      begin
        return Oj.load(response[:content])
      rescue SyntaxError, Oj::ParseError => e
      end
      return false
    end

    def attempt_elastic(params)
      begin
        @ua.reset
        rsp = @ua.send(*params)
        return rsp
      rescue => e
      end
      return false
    end

  end

end
