# encoding:utf-8

module Request

  CONTENT_TYPE = {:content_type => 'application/json'}

  private

  def request_elastic(method, url, data = nil, params = {})
    req_params = []
    req_params.push(method, url)
    req_params << data   if data && !data.empty?
    req_params << params if params && !params.empty?

    GLogg.l_f{ req_params }

    content_type_position = method == :get ? 2 : 3
    if req_params[content_type_position].nil?
      req_params << CONTENT_TYPE
    else
      req_params[-1].merge(CONTENT_TYPE)
    end

    response = nil
    GLogg.l_i {
      "ElasticSearch.attempt_elastic: \nmethod: #{method}\nurl: '#{url}'\n" +
      "#{data && !data.empty?     ? "query: '#{data}'\n" : ''}" +
      "#{params && !params.empty? ? "params: '#{data}'" : ''}"
    }
    response = attempt_elastic req_params

    unless response && !response[:error]
      return false if method == :get && response[:error] == "404 Not Found"
      GLogg.l_f { "ElasticSearch.request_elastic: Failed #{req_params}\n " +
        " Last error: '#{response[:error] if response && response.kind_of?(Hash)}'\n" +
        " Response:\n '#{response[:content] if response && response.kind_of?(Hash)}'"
      }
      return false
    end

    begin
      return Oj.load(response[:content])
    rescue SyntaxError, Oj::ParseError => e
      GLogg.l_e { "ElasticSearch.request_elastic: Oj.load failed "}
    end
    return false
  end

  def attempt_elastic(params)
    begin
      @ua.reset
      rsp = @ua.send(*params)
      return rsp
    rescue => e
      GLogg.l_f {
        "ElasticSearch.attempt_elastic: #{params} : '#{e.message}'" +
        " \n #{e.backtrace.inspect}"
      }
    end
    return false
  end

end
