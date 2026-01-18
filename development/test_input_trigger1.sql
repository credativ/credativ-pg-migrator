CREATE TRIGGER dbo.address_validation_i_trigger
    ON ccd.dbo.address_validation
    FOR INSERT
    AS

BEGIN
    -- declare local variables
    DECLARE @contact_id NUMERIC(10, 0)
    DECLARE @address_validation_date DATETIME
    DECLARE @task_state_initial UNIVARCHAR(150)

    -- get contact_id and address_validation_date
    SELECT @contact_id = fk_contact_id,
           @address_validation_date = address_validation_date
    FROM inserted
    -- set task_state_initial
    SELECT @task_state_initial = 'WAITING_FOR_PROCESS_START'
    --------------------------------------------------------------------------------------------------------------------------
    -- INSERT INTO address_validation_task TABLE
    IF (NOT EXISTS(SELECT * FROM ccd.dbo.address_validation_task avt where avt.fk_contact_id = @contact_id))
        BEGIN
            INSERT INTO ccd.dbo.address_validation_task(fk_contact_id, address_validation_date, task_state)
            VALUES (@contact_id, @address_validation_date, @task_state_initial)
        END
    ELSE
        BEGIN
            -- declare local variables
            DECLARE @current_task_state UNIVARCHAR(150)
            -- get task state of the existing entry
            SELECT @current_task_state =
                   (SELECT task_state FROM ccd.dbo.address_validation_task avt where avt.fk_contact_id = @contact_id)
            IF (@current_task_state != 'PROCESS_STARTED')
                BEGIN
                    UPDATE ccd.dbo.address_validation_task
                    SET address_validation_date = @address_validation_date,
                        task_state              = @task_state_initial,
                        task_error_message      = NULL,
                        task_end_reason         = NULL
                    WHERE fk_contact_id = @contact_id
                END
        END
    -- END INSERT INTO address_validation_task TABLE
------------------------------------------------------------------------------------------------------------------------
    RETURN
END
