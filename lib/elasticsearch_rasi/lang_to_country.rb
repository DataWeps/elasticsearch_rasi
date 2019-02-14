# encoding:utf-8
require_relative 'document'
class ElasticsearchRasi
  module LangToCountry
    LANG_TO_COUNTRY = {
      'cs'  => 'cze',
      'sk'  => 'svk',
      'en'  => 'eng',
      'hu'  => 'hun',
      'pl'  => 'pol',
      'de'  => 'deu',
      'es'  => 'mex',
      'mk'  => 'mkd',
      'ru'  => 'rus',
      'lv'  => 'lva',
      'lt'  => 'ltu',
      'sr'  => 'srb',
      'bs'  => 'bih',
      'hr'  => 'hrv',
      'fr'  => 'fra',
      'it'  => 'ita' }.freeze

    def translate_lang_to_country(lang)
      if lang
        return [] << LANG_TO_COUNTRY[lang[0]] if lang.is_a?(Array)
        [] << LANG_TO_COUNTRY[lang]
      end
    end
  end
end
