-- mark that our application package depends on an external database in
-- the provider account. By granting "reference_usage", the proprietary data
-- in the provider_data database can be shared through the app
grant reference_usage on database MARKETING_DATA_FOUNDATAION
    to share in application package {{ package_name }};

-- now that we can reference our proprietary data, let's create some views
-- this "package schema" will be accessible inside of our setup script
create schema if not exists {{ package_name }}.SHARED_CONTENT;
use schema {{ package_name }}.SHARED_CONTENT;
grant usage on schema {{ package_name }}.SHARED_CONTENT
  to share in application package {{ package_name }};

-- Our actual data share. Only visible to APP_PRIMARY without further grants.
create view if not exists SHARED_CONTENT.C360_CLICKS_CRM_JOINED_VW as
    select *
    from MARKETING_DATA_FOUNDATAION.DEMO.C360_CLICKS_CRM_JOINED_VW;

create view if not exists SHARED_CONTENT.CAMPAIGN72_VIEW as
    select *
    from MARKETING_DATA_FOUNDATAION.DEMO.CAMPAIGN72_VIEW;

grant select on view SHARED_CONTENT.C360_CLICKS_CRM_JOINED_VW
  to share in application package {{ package_name }};
grant select on view SHARED_CONTENT.CAMPAIGN72_VIEW
  to share in application package {{ package_name }};