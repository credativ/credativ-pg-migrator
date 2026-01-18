CREATE TRIGGER dbo.tr_validity_duration_defaults_i
on ccd.dbo.validity_duration_defaults
for insert
as

begin
	-- declare local variables */
    declare @ts_init        datetime
    declare @loginname      univarchar(15)

    -- get current system time and login role and login name */
    select @ts_init     = getdate(),
           @loginname   = substring(suser_name(), 1, 15)


    -- INSERT INTO HIS TABLE
    insert into ccd.dbo.his_validity_duration_defaults(his_ts_init
                                                   ,his_login_id
                                                   ,his_change_type
                                                   ,validity_duration_defaults_id
                                                   ,permission_target_id
                                                   ,permission_status_id
                                                   ,duration_in_months
                                                   ,description
                                                   ,valid_from
                                                   ,valid_to
                                                   ,executor
                                                   ,ts_init
                                                   ,ts_update
     )
     select @ts_init
           ,@loginname
           ,'I'
           ,validity_duration_defaults_id
           ,permission_target_id
           ,permission_status_id
           ,duration_in_months
           ,description
           ,valid_from
           ,valid_to
           ,executor
           ,ts_init
           ,ts_update
    from inserted

    if(@@error <> 0)
    begin
        -- error occured
       rollback trigger with raiserror 99999 'tr_validity_duration_defaults_i: Error inserting history entry'
    end
    -- END HIS TABLE

    return

end
