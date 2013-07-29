# -- coding: utf-8
require "uri"
require "open-uri"
require "rubygems"
require "sequel"
require "nokogiri"
require "pathname"

# ruby crowler.rb http://google.com

module SqliteStore
  def initStore
    @db = Sequel.sqlite "urlitem.sqlite"

    @db.create_table? :items do
      primary_key :id
      String :title
      String :domain
      String :path
      Integer :done
      Integer :error
    end
    
    @items = @db[:items]
  end

  def storeData(datas)
    datas.each do |item|
      i = @items[path: item]

      if i.nil? or i.count == 0
        p item
        @items.insert done: 0, error: 0, domain: @domain, path: item, title: ""
      end
    end
  end

  def getNextUrl
    i = @items.first( domain: @domain, done: 0 )
    j = @items.first( domain: @domain, done: 1 )
    if i.nil?
      if j.nil?
        {domain: @domain, path: "/"}
      else
        p "DONE..."
        exit
      end
    else
      i
    end
  end

  def flgAsDone(obj)
    d = @items[domain: obj[:domain], path: obj[:path]]
    if not d.nil?
      @db[:items].filter(domain: obj[:domain], path: obj[:path]).update(done: 1, title: @title)
    end
  end

  def flgAsError(obj)
    @db[:items].filter(domain: obj[:domain], path: obj[:path]).update(error: 1, done:1)
  end
end


module ExtractHrefWithDom
  def getHref(url, html, charset)
    doc = Nokogiri::HTML.parse(html, nil, charset)

    @title = doc.title

    taga = doc.css( "a" )
    p ""
    p taga.count.to_s + " links found."
    p ""
    href = taga.map do |item|
      h = item.attribute("href")
      if not h.nil?
        self.filterHref url, h.value
      end
    end
    href.compact
  end

  def filterHref(url, href)
    if href.match /(\.pdf|\.zip|\.jpg|\.swf|\.asx|\.gif|\.mpeg|\.wmv|\.mp3|\.wav|\.bmp|\.rm|\.mov|\.avi|\.tiff|\.tif|\.exe|\.dmg)$/ 
      return nil
    elsif href.match /javascript/
      return nil
    else 
      begin
        uriResolved = URI.join( url, href.sub( /#.*$/, "" ) ).to_s
      rescue URI::InvalidURIError
        return nil
      end

      if uriResolved.match( @domain )
        path = "/"+uriResolved.split('/').slice(3..-1).join('/')
        return self.addSlash path
      else
        return nil
      end
    end
  end

  def addSlash(path)
    # 対象URLがディレクトリだった場合、末尾にスラッシュを加える
    # でないと URI.join(//www.example.com/en, program.html)が/program.html になる
    splitted = path.split('/')
    if not splitted.empty? and splitted.last.match(/^[\w\-_]+$/)
      # 末尾の要素にドットが含まれていなかったら/追加
      splitted.join('/')+"/"
    else
      path
    end
  end

end


class Crowl 

  include ExtractHrefWithDom, SqliteStore

  def initialize(domain)
    @domain = domain
    self.initStore
  end
    
  def start
    url = self.getNextUrl
    p ""
    p ""
    p ""
    p "FETCH ===== "+ url[:path]
    
    begin
      datas = self.fetch url[:domain]+url[:path]
    rescue OpenURI::HTTPError => err
      # 404エラーなど
      p err
      self.flgAsError url
    rescue RuntimeError, SocketError, Errno::ETIMEDOUT
      # httpからhttpsへのリダイレクト時に起きる redirection forbidden
      # URLパラメータにurl含まれている
      p err
      self.flgAsError url
    rescue Timeout::Error
      # タイムアウト時は3秒待ってリトライ
      sleep(3)
      self.start()
    else
      if not datas.nil?
        self.storeData datas
      end
      self.flgAsDone url
    end
    sleep(0.3)

    self.start()
    #再帰でよみこむ
  end

  def fetch(url)
    charset = nil
    # タイムアウトは10秒
    prevurl = url
    html = open(url, read_timeout: 10) do |f|
      charset = f.charset
      url = f.base_uri.to_s
      f.read
    end

    if prevurl != url
      p "  ###  redirected.. "+url
    end

    self.getHref url, html, charset
  end

end

require "optparse"
opt = OptionParser.new

opt.permute!(ARGV)
domainToCrowl = ARGV.join ""

if domainToCrowl != ""
  c = Crowl.new domainToCrowl
  c.start()
end

