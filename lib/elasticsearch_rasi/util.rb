#encoding:utf-8
require 'cgi'

module Util

  # convert all keys in the hash to string
  def self.hash_keys_to_str h
    new_h = {}
    h.each_pair { |k, v| new_h[k.to_s] = h[k] }
    new_h
  end

  def self.hash_str_to_hashes h
    new_h = {}
    h.each_pair { |k, v| new_h[k.to_sym] = h[k] }
    new_h
  end

  # assemble additional parameter string from options in hash
  # e.g. {'arg1' => 'val1', 'arg2' => 'val2'} => 'arg1=val1&arg2=val2'
  def self.param_str(h, join = '&')
    arr = []
    h.each_pair{|p,arg| arr.push "#{CGI.escape p.to_s}=#{CGI.escape arg.to_s}" }
    arr.join join
  end

  def self.parse_date_offset offset
    if offset.kind_of?(Integer) || offset =~ /^\d+(?:h(?:ours?)?)?$/i
      return Time.at(Time.now - offset.to_i * 3600)
    elsif offset =~ /^\d+d(?:ays?)?$/i
      return Time.at(Time.now - offset.to_i * 86400)
    elsif offset =~ /^20\d{2}-[01]\d-[0-3]\d$/
      return Time.local(*offset.split('-'))
    else # should not happen
      raise ArgumentError.new(
        "Failed to parse time/offset specification '#{offset}'"
      )
    end
  end

end # Util

