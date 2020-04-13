require 'apartment/migrator'
require 'parallel'

apartment_namespace = namespace :apartment do

  desc "Create all tenants"
  task create: ['db:load_config'] do
    tenants.each do |tenant|
      begin
        puts("Creating #{tenant} tenant")
        Apartment::Tenant.create(tenant)
      rescue Apartment::TenantExists => e
        puts e.message
      end
    end
  end

  desc "Drop all tenants"
  task drop: ['db:load_config', 'db:check_protected_environments'] do
    tenants.each do |tenant|
      begin
        puts("Dropping #{tenant} tenant")
        Apartment::Tenant.drop(tenant)
      rescue Apartment::TenantNotFound => e
        puts e.message
      end
    end
  end

  desc "Migrate all tenants"
  task migrate: [:environment, 'db:load_config'] do
    warn_if_tenants_empty
    each_tenant do |tenant|
      begin
        puts("Migrating #{tenant} tenant")
        Apartment::Migrator.migrate tenant
      rescue Apartment::TenantNotFound => e
        puts e.message
      end
    end
    apartment_namespace["_dump"].invoke
  end

  desc "Seed all tenants"
  task :seed do
    warn_if_tenants_empty

    each_tenant do |tenant|
      begin
        puts("Seeding #{tenant} tenant")
        Apartment::Tenant.switch(tenant) do
          Apartment::Tenant.seed
        end
      rescue Apartment::TenantNotFound => e
        puts e.message
      end
    end
  end

  desc "Rolls the migration back to the previous version (specify steps w/ STEP=n) across all tenants."
  task rollback: [:environment, 'db:load_config'] do
    warn_if_tenants_empty

    step = ENV['STEP'] ? ENV['STEP'].to_i : 1

    each_tenant do |tenant|
      begin
        puts("Rolling back #{tenant} tenant")
        Apartment::Migrator.rollback tenant, step
      rescue Apartment::TenantNotFound => e
        puts e.message
      end
    end
    apartment_namespace["_dump"].invoke
  end

  namespace :migrate do
    desc 'Runs the "up" for a given migration VERSION across all tenants.'
    task up: [:environment, 'db:load_config'] do
      warn_if_tenants_empty

      version = ENV['VERSION'] ? ENV['VERSION'].to_i : nil
      raise 'VERSION is required' unless version

      each_tenant do |tenant|
        begin
          puts("Migrating #{tenant} tenant up")
          Apartment::Migrator.run :up, tenant, version
        rescue Apartment::TenantNotFound => e
          puts e.message
        end
      end
      apartment_namespace["_dump"].invoke
    end

    desc 'Runs the "down" for a given migration VERSION across all tenants.'
    task down: [:environment, 'db:load_config'] do
      warn_if_tenants_empty

      version = ENV['VERSION'] ? ENV['VERSION'].to_i : nil
      raise 'VERSION is required' unless version

      each_tenant do |tenant|
        begin
          puts("Migrating #{tenant} tenant down")
          Apartment::Migrator.run :down, tenant, version
        rescue Apartment::TenantNotFound => e
          puts e.message
        end
      end
      apartment_namespace["_dump"].invoke
    end

    desc  'Rolls back the tenant one migration and re migrate up (options: STEP=x, VERSION=x).'
    task redo: [:environment, 'db:load_config'] do
      if ENV['VERSION']
        apartment_namespace['migrate:down'].invoke
        apartment_namespace['migrate:up'].invoke
      else
        apartment_namespace['rollback'].invoke
        apartment_namespace['migrate'].invoke
      end
    end
  end

  namespace :schema do
    desc "Creates a `database_schema_file` file that is portable against any DB supported by Active Record"
    task dump: [:environment, 'db:load_config'] do
      apartment_namespace["_dump"].invoke
    end
  end

  namespace :structure do
    desc "Dumps the database structure to `database_schema_file`."
    task dump: [:environment, 'db:load_config'] do
      apartment_namespace["_dump"].invoke
    end
  end

  # IMPORTANT: This task won't dump the schema if ActiveRecord::Base.dump_schema_after_migration is set to false
  task :_dump do
    if ActiveRecord::Base.dump_schema_after_migration
      filename = Rails.root.join(Apartment.database_schema_file).to_s
      Apartment::Tenant.switch(Apartment.default_tenant_name) do
        case Apartment.schema_format
        when :ruby then
          File.open(filename, 'w:utf-8') do |file|
            ActiveRecord::SchemaDumper.ignore_tables = Apartment.excluded_models.collect { |m| m.constantize.table_name.split('.').last }
            ActiveRecord::SchemaDumper.dump(ActiveRecord::Base.connection, file)
          end
        when :sql then
          current_config = ActiveRecord::Base.connection_config.stringify_keys
          ActiveRecord::Tasks::DatabaseTasks.structure_dump(current_config, filename)
          if ActiveRecord::SchemaMigration.table_exists?
            File.open(filename, "a") do |f|
              f.puts ActiveRecord::Base.connection.dump_schema_information
              f.print "\n"
            end
          end
        else
          raise "unknown schema format #{Apartment.schema_format}"
        end
      end
    end
    # Allow this task to be called as many times as required. An example is the
    # migrate:redo task, which calls other two internally that depend on this one.
    apartment_namespace["_dump"].reenable
  end

  def each_tenant(&block)
    Parallel.each(tenants, in_threads: Apartment.parallel_migration_threads) do |tenant|
      block.call(tenant)
    end
  end

  def tenants
    ENV['DB'] ? ENV['DB'].split(',').map { |s| s.strip } : Apartment.tenant_names || []
  end

  def warn_if_tenants_empty
    if tenants.empty? && ENV['IGNORE_EMPTY_TENANTS'] != "true"
      puts <<-WARNING
        [WARNING] - The list of tenants to migrate appears to be empty. This could mean a few things:

          1. You may not have created any, in which case you can ignore this message
          2. You've run `apartment:migrate` directly without loading the Rails environment
            * `apartment:migrate` is now deprecated. Tenants will automatically be migrated with `db:migrate`

        Note that your tenants currently haven't been migrated. You'll need to run `db:migrate` to rectify this.
      WARNING
    end
  end
end
