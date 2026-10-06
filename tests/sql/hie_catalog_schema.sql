CREATE TABLE <<schema>>.MASTER_WAREHOUSE_RUN (
    ID int IDENTITY(1,1) NOT NULL CONSTRAINT PK_<<name>>_MWR PRIMARY KEY,
    PARENT_RUN_ID int NULL,
    RUN_TYPE nvarchar(200) NOT NULL,
    RUN_ID uniqueidentifier NOT NULL DEFAULT (newid()),
    COMPLETION_STATUS nvarchar(2000) NULL,
    DATE_COMPLETED datetime NULL,
    DATE_STARTED datetime NULL DEFAULT (getdate()),
    NOTES nvarchar(4000) NULL,
    RUN_DESCRIPTION nvarchar(200) NULL,
    RESULT nvarchar(2000) NULL,
    [USER] nvarchar(50) NOT NULL DEFAULT (suser_sname()),
    XML_CONFIG xml NULL,
    XML_OUTPUT xml NULL,
    PROJECT varchar(1000) NULL,
    RUN_CLASS varchar(100) NULL
)
GO
CREATE TABLE <<schema>>.DATA_FILE (
    ID int IDENTITY(1,1) NOT NULL CONSTRAINT PK_<<name>>_DF PRIMARY KEY,
    RUN_ID int NOT NULL,
    FILE_SET varchar(500) NULL,
    FILE_TYPE varchar(500) NULL,
    SOURCE_URI varchar(2000) NULL,
    SOURCE_VERSION_NAME varchar(500) NULL,
    SOURCE_VERSION_NUM varchar(500) NULL,
    FILE_NAME varchar(2000) NULL,
    FILE_REL_PATH varchar(2000) NULL,
    RUN_TYPE_ROOT_DIR varchar(2000) NULL,
    PARENT_FILE int NULL,
    FILE_SIZE bigint NULL,
    FILE_HASH varchar(1000) NULL,
    DATE_LOADED datetime NULL,
    DATE_MODIFIED datetime NULL,
    DATE_CREATED datetime NULL,
    EXCEPTIONS varchar(8000) NULL
)
