require 'cassandra'

module Cassanova
  class Model
    extend  ActiveModel::Naming
    extend  ActiveModel::Translation
    include ActiveModel::AttributeMethods
    include ActiveModel::Validations
    include ActiveModel::Conversion

    ### Initializing Methods ###

    # define_attribute_methods COLUMNS

    cattr_reader :cassandra_columns, :cassandra_hosts, :cassandra_database

    def initialize data={}
      cassandra_columns.each{|c| send("#{c}=", data[c.to_s]) }
    end

    def attributes
      attrs = {}
      cassandra_columns.each{|m| attrs[m] = send(m) }
      return attrs
    end

    def assign_attributes data={}
      data.each do |k,v|
        begin
          send("#{k}=", v)
        rescue
          # only assign valid attributes
        end
      end
    end

    def self.cluster
      @@cassandra_cluster ||= Cassandra.cluster(hosts: cassandra_hosts)
    end

    def self.session
      @@cassandra_session ||= cluster.connect(cassandra_database)
    end

    def self.execute *query
      session.execute(query)
    end

    def self.prepare query
      session.prepare(query)
    end

    ### Query Methods ###

    def self.where conditions={}
      query = Cassanova::Query.new(:table_name => self.name.underscore.pluralize)
      query.where(conditions)
      return query
    end

    def self.select *attrs
      query = Cassanova::Query.new(:table_name => self.name.underscore.pluralize)
      query.select(attrs)
      return query
    end

    ### Config Methods ###

    def self.columns *syms
      # unless defined? @@cassandra_columns
        @@cassandra_columns = syms if syms.present?
        @@cassandra_columns.each{|c| attr_accessor(c) }
      # end
    end

    def self.hosts *host_names
      # unless defined? @@cassandra_hosts
        @@cassandra_hosts = host_names if host_names.present?
      # end
    end

    def self.database db
      # unless defined? @@cassandra_database
        @@cassandra_database = db if db.present?
      # end
    end

  end

  class Query
    attr_accessor :query, :query_type, :queries, :table_name

    def initialize options={}
      @query = options[:query]
      @query_type = options[:query_type]
      @table_name = options[:table_name]
    end

    def where conditions={}
      query = []
      conditions.each do |key, val|
        query << "#{key} = #{val}"
      end
      self.queries ||= []
      self.queries << Cassanova::Query.new(:query_type => "where", :query => query.join(" AND "))
      return self
    end

    def select *attrs
      self.queries ||= []
      self.queries << Cassanova::Query.new(:query_type => "select", :query => attrs.join(','))
      return self
    end

    def limit i
      cq = compiled_query + " LIMIT #{i}"
      Cassanova::Query.parse(Cassanova::Model.session.execute(cq), table_name)
    end

    def first
      limit(1).first
    end

    def all
       Cassanova::Query.parse(Cassanova::Model.session.execute(compiled_query), table_name)
    end

    def count
      cq = compiled_query
      selects = cq.split("select ")[1].split(" from")[0]
      cq = cq.gsub(selects, "COUNT(*)")
      Cassanova::Model.session.execute(cq).rows.first['count']
    end

    def self.parse response, table_name
      table_class = table_name.classify.constantize
      objs = []
      response.each do |d|
        objs << table_class.new(d)
      end
      return objs
    end

    def compiled_query
      wheres = []
      selects = []
      queries.each do |query|
        case query.query_type
        when "where"
          wheres << query.query
        when "select"
          selects << query.query
        end
      end
      query = "select #{selects.present? ? selects.join(',') : '*'} from #{table_name}"
      if wheres.present?
        query += " where #{wheres.join(' AND ')}"
      end
    end

  end
end
