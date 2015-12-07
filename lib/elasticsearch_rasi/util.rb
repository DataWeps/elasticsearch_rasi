#encoding:utf-8
require 'cgi'

class ElasticsearchRasi
  module Util
    # assemble additional parameter string from options in hash
    # e.g. {'arg1' => 'val1', 'arg2' => 'val2'} => 'arg1=val1&arg2=val2'
    def self.param_str(h, join = '&')
      arr = []
      h.each_pair{ |p, arg| arr << "#{CGI.escape(p.to_s)}=#{CGI.escape(arg.to_s)}" }
      arr.join(join)
    end

    def self.parse_date_offset(offset)
      now_time = now
      if offset.is_a?(Integer) || offset =~ /^\d+(?:h(?:ours?)?)?$/i
        return Time.at(now_time.to_time - offset.to_i * 3600)
      elsif offset =~ /^\d+d(?:ays?)?$/i
        return Time.at(now_time.to_time - offset.to_i * 86_400)
      elsif offset =~ /^\d+m(?:months?)?$/i
        new_time = now_time << offset.to_i
        # new_time  = DateTime.now << offset.to_i
        return Time.at(new_time.to_time)
      elsif offset =~ /^20\d{2}-[01]\d-[0-3]\d$/
        return Time.local(*offset.split('-'))
      else # should not happen
        begin
          return Time.parse(offset)
        rescue ArgumentError
          raise "Failed to parse time/offset specification '#{offset}'"
        end
      end
    end

    def self.now
      DateTime.now
    end
  end
end
