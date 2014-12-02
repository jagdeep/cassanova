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
      params = {}
      data.each{|k,v| params[k.to_s] = v }
      cassandra_columns.each{|c| send("#{c}=", params[c.to_s]) }
    end

    def attributes
      attrs = {}
      cassandra_columns.each{|m| attrs[m.to_s] = send(m) }
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

    def self.create data={}
      obj = self.new(data)
      obj.created_at = Time.zone.now if obj.attributes.include?("created_at")
      obj.updated_at = Time.zone.now if obj.attributes.include?("updated_at")
      if obj.valid?
        cols = obj.attributes.keys
        vals = cols.map{|k| obj.send(k) }
        query = "INSERT INTO #{self.name.underscore.pluralize} (#{cols.join(', ')}) VALUES (#{vals.map{'?'}.join(', ')})"
        begin
          query = Cassanova::Model.session.prepare(query)
          result = Cassanova::Model.session.execute(*[query, vals].flatten)
          return result.class == Cassandra::Results::Void
        rescue Exception => e
          obj.errors.add(:cassandra, e.message)
          return obj
        end
      else
        return obj
      end
    end

    ### Relationship Methods ###

    def self.belongs_to model_name
      define_method(model_name.to_s) do
        model_name.to_s.classify.constantize.where(:id => send("#{model_name}_id")).first
      end
    end

    def self.has_many table_name
      define_method(table_name.to_s) do
        table_name.to_s.classify.constantize.where("#{self.class.name.underscore}_id" => send("id")).first
      end
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

    def destroy
      cq = compiled_query
      cq = "delete from " + cq.split("from")[1]
      result = Cassanova::Model.session.execute(cq)
      return result.class == Cassandra::Results::Void
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
