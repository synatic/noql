SELECT DISTINCT 
	[Uniq Client ID],
	[Lookup Code],
	[Client Name],
	UniqActivity,
	ActivityCode,
	[Activity Description],
	[Who/Owner],
	ID,
	Event_Uniq,
	Event_Date,
	CONVERT(varchar, IIF(Event_Type = 1, 'Activity Created', 
		IIF(Event_Type = 2, 'Activity Closed', 
		IIF(Event_Type = 3, 'Note Created',
		IIF(Event_Type = 4, 'Task Created', 
		IIF(Event_Type = 5, 'Attachment Created', '')))))) AS 'Event Type', 
	Event_Desc,
	IIF(Event_Type = 5, FileExtension,'') AS 'File Type',
	Activity_Status,
	[Activity Dept],
	[Branch],
	[Profit Center],
	[RiskAdvisor],
	[AccountMgr],
	[RACode],
	[AMCode],
	GETDATE() AS 'Last Refreshed'

FROM
		(SELECT
			dbo.Client.UniqEntity AS 'Uniq Client ID',
			dbo.Client.LookupCode AS 'Lookup Code',
			dbo.Client.NameOf AS 'Client Name',
			dbo.Activity.UniqActivity,
			dbo.ActivityCode.ActivityCode,
			IIF(dbo.Activity.UniqEmployee = -1,dbo.Workgroup.LookupCode,Activity_Owner.LookupCode) AS 'Who/Owner',
			dbo.Activity.DescriptionOf AS 'Activity Description',
			IIF(dbo.Activity.ClosedDate IS NULL, 'Open', 'Closed') AS Activity_Status,
			IIF(dbo.Activity.UniqDepartment <> -1,Act_Dept.DepartmentCode,Pol_Dept.DepartmentCode) AS 'Activity Dept',
			IIF(dbo.Activity.UniqBranch <> -1,Act_Branch.BranchCode,Pol_Branch.BranchCode) AS 'Branch',
			IIF(dbo.Activity.UniqProfitCenter <> -1,Act_PC.ProfitCenterCode,Pol_PC.ProfitCenterCode) AS 'Profit Center',
		
			IIF(dbo.ActivityTask.StatusCode IS NULL, NULL,
				IIF(dbo.ActivityTask.StatusCode = 'I', 'In Progress',
				IIF(dbo.ActivityTask.StatusCode = 'A', 'Cancelled',
				IIF(dbo.ActivityTask.StatusCode = 'N', 'Not Started',
				IIF(dbo.ActivityTask.StatusCode = 'P', 'Marked N/A',
				IIF(dbo.ActivityTask.StatusCode = 'C', 'Complete', 'PICNIC')))))) AS Task_Status,
		
			CONVERT(varchar(50),dbo.Activity.InsertedByCode) AS 'Act_Ins_By',
			CONVERT(varchar(50),dbo.Activity.ClosedByCode) AS 'Act_Cls_By',
			CONVERT(varchar(50),dbo.ActivityNote.InsertedByCode) AS 'Note_Ins_By',
			CONVERT(varchar(50),dbo.ActivityTask.InsertedByCode) AS 'Task_Ins_By',
			CONVERT(varchar(50),dbo.Attachment.InsertedByCode) AS 'Att_Ins_By',

			Act_Ins_ID.UniqSecurityUser AS 'Act_Ins_ID',
			Act_Cls_ID.UniqSecurityUser AS 'Act_Cls_ID',
			Note_Ins_ID.UniqSecurityUser AS 'Note_Ins_ID',
			Task_Ins_ID.UniqSecurityUser AS 'Task_Ins_ID',
			Att_Ins_ID.UniqSecurityUser AS 'Att_Ins_ID',
		
			DATEADD(HOUR, - 8, dbo.Activity.InsertedDate) AS 'Act_Ins_Date',
			DATEADD(HOUR, - 8, dbo.Activity.ClosedDate) AS 'Act_Cls_Date',
			DATEADD(HOUR, - 8, dbo.ActivityNote.InsertedDate) AS 'Note_Ins_Date',
			DATEADD(HOUR, - 8, dbo.ActivityTask.InsertedDate) AS 'Task_Ins_Date',
			DATEADD(HOUR, - 8, dbo.Attachment.AttachedDate) AS 'Att_Ins_Date',
		
			IIF(dbo.Activity.InsertedDate IS NULL, NULL, 1) AS 'Act_Ins_Event',
			IIF(dbo.Activity.ClosedDate IS NULL, NULL, 2) AS 'Act_Cls_Event',
			IIF(dbo.ActivityNote.InsertedDate IS NULL, NULL, 3) AS 'Note_Ins_Event',
			IIF(dbo.ActivityTask.InsertedDate IS NULL, NULL, 4) AS 'Task_Ins_Event',
			IIF(dbo.Attachment.AttachedDate IS NULL, NULL, 5) AS 'Att_Ins_Event',
		
			CONVERT(varchar(100), dbo.Activity.DescriptionOf) AS 'Act_Ins_Desc',
			CONVERT(varchar(100), dbo.Activity.DescriptionOf) AS 'Act_Cls_Desc',
			CONVERT(varchar(100), dbo.ActivityNote.Note) AS 'Note_Ins_Desc',
			CONVERT(varchar(100), dbo.ActivityTask.DescriptionOf) AS 'Task_Ins_Desc',
			CONVERT(varchar(100), CONCAT(dbo.Attachment.DescriptionOf,dbo.Attachment.FileExtension)) AS 'Att_Ins_Desc',

			dbo.Activity.UniqActivity AS 'Act_Ins_Uniq',
			dbo.Activity.UniqActivity AS 'Act_Cls_Uniq',
			dbo.ActivityNote.UniqActivityNote AS 'Note_Ins_Uniq',
			dbo.ActivityTask.UniqActivityTask AS 'Task_Ins_Uniq',
			dbo.Attachment.UniqAttachment AS 'Att_Ins_Uniq',
			dbo.Attachment.FileExtension,
			Svc_Role.RiskAdvisor,
			Svc_Role.AccountMgr,
			Svc_Role.RACode,
			Svc_Role.AMCode
			

		FROM dbo.Activity
			LEFT OUTER JOIN dbo.Employee AS Activity_Owner ON dbo.Activity.UniqEmployee = Activity_Owner.UniqEntity
			LEFT OUTER JOIN dbo.Workgroup ON dbo.Activity.UniqWorkGroup = dbo.Workgroup.UniqWorkGroup
			LEFT OUTER JOIN dbo.ConfigureLkLanguageResource AS Workgroup_Name ON dbo.Workgroup.ConfigureLkLanguageResourceID = Workgroup_Name.ConfigureLkLanguageResourceID AND Workgroup_Name.CultureCode = 'en-US'
			FULL OUTER JOIN dbo.ActivityCode ON dbo.Activity.UniqActivityCode = dbo.ActivityCode.UniqActivityCode
			LEFT OUTER JOIN dbo.Client ON dbo.Activity.UniqEntity = dbo.Client.UniqEntity
			LEFT OUTER JOIN dbo.Department AS Act_Dept ON dbo.Activity.UniqDepartment = Act_Dept.UniqDepartment
			LEFT OUTER JOIN dbo.Branch AS Act_Branch ON dbo.Activity.UniqBranch = Act_Branch.UniqBranch
			LEFT OUTER JOIN dbo.ProfitCenter AS Act_PC ON dbo.Activity.UniqProfitCenter = Act_PC.UniqProfitCenter
			
			LEFT OUTER JOIN dbo.SecurityUser AS Act_Ins_ID ON dbo.Activity.InsertedByCode = Act_Ins_ID.UserCode
			LEFT OUTER JOIN dbo.SecurityUser AS Act_Cls_ID ON dbo.Activity.InsertedByCode = Act_Cls_ID.UserCode
			
			FULL OUTER JOIN dbo.ActivityNote ON dbo.Activity.UniqActivity = dbo.ActivityNote.UniqActivity --AND (dbo.ActivityNote.InsertedDate BETWEEN DATEADD(MONTH,-13,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0)) AND EOMONTH(DATEADD(MONTH,-2,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0))))
			LEFT OUTER JOIN dbo.SecurityUser AS Note_Ins_ID ON dbo.ActivityNote.InsertedByCode = Note_Ins_ID.UserCode

			LEFT OUTER JOIN dbo.ActivityTask ON dbo.Activity.UniqActivity = dbo.ActivityTask.UniqActivity --AND (dbo.ActivityTask.InsertedDate BETWEEN DATEADD(MONTH,-13,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0)) AND EOMONTH(DATEADD(MONTH,-2,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0))))
			LEFT OUTER JOIN dbo.SecurityUser AS Task_Ins_ID ON dbo.ActivityTask.InsertedByCode = Task_Ins_ID.UserCode

			LEFT OUTER JOIN dbo.AttachmentAttachedTo ON dbo.Activity.UniqActivity = dbo.AttachmentAttachedTo.UniqActivity
			LEFT OUTER JOIN dbo.Attachment ON dbo.AttachmentAttachedTo.UniqAttachment = dbo.Attachment.UniqAttachment --AND ((NOT(dbo.AttachmentAttachedTo.UniqActivity = - 1)) AND (dbo.Attachment.AttachedDate BETWEEN DATEADD(MONTH,-13,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0)) AND EOMONTH(DATEADD(MONTH,-2,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0)))))
			LEFT OUTER JOIN dbo.SecurityUser AS Att_Ins_ID ON dbo.Attachment.InsertedByCode = Att_Ins_ID.UserCode
			LEFT OUTER JOIN dbo.ActivityPolicyLineJT ON dbo.Activity.UniqActivity = dbo.ActivityPolicyLineJT.UniqActivity

			LEFT OUTER JOIN dbo.Policy ON dbo.ActivityPolicyLineJT.UniqPolicy = dbo.Policy.UniqPolicy
			LEFT OUTER JOIN dbo.Line ON dbo.Policy.UniqPolicy = dbo.Line.UniqPolicy
			LEFT OUTER JOIN dbo.Department AS Pol_Dept ON dbo.Activity.UniqDepartment = Pol_Dept.UniqDepartment
			LEFT OUTER JOIN dbo.Branch AS Pol_Branch ON dbo.Activity.UniqBranch = Pol_Branch.UniqBranch
			LEFT OUTER JOIN dbo.ProfitCenter AS Pol_PC ON dbo.Activity.UniqProfitCenter = Pol_PC.UniqProfitCenter

			LEFT OUTER JOIN
				(SELECT 
							dbo.Line.UniqPolicy, 
							MAX(CASE WHEN dbo.CdServicingRole.CdServicingRoleCode = 'RA0' THEN dbo.Employee.NameOf ELSE NULL END) AS RiskAdvisor, 
							MAX(CASE WHEN dbo.CdServicingRole.CdServicingRoleCode = 'AM0' THEN dbo.Employee.NameOf ELSE NULL END) AS AccountMgr,
							MAX(CASE WHEN dbo.CdServicingRole.CdServicingRoleCode = 'RA0' THEN dbo.SecurityUser.UniqSecurityUser ELSE NULL END) AS RACode, 
							MAX(CASE WHEN dbo.CdServicingRole.CdServicingRoleCode = 'AM0' THEN dbo.SecurityUser.UniqSecurityUser ELSE NULL END) AS AMCode
						FROM dbo.Line
							LEFT OUTER JOIN dbo.LineEmployeeServicingJT ON dbo.Line.UniqLine = dbo.LineEmployeeServicingJT.UniqLine
							LEFT OUTER JOIN dbo.CdServicingRole ON dbo.CdServicingRole.UniqCdServicingRole = dbo.LineEmployeeServicingJT.UniqCdServicingRole 
							AND dbo.CdServicingRole.CdServicingRoleCode IN ('RA0', 'AM0','AA0','BC0','MA0')
							LEFT OUTER JOIN dbo.Employee ON dbo.LineEmployeeServicingJT.UniqEntity = dbo.Employee.UniqEntity
							LEFT OUTER JOIN dbo.SecurityUser ON dbo.Employee.UniqEntity = dbo.SecurityUser.UniqEmployee
						GROUP BY dbo.Line.UniqPolicy
			) AS Svc_Role ON dbo.ActivityPolicyLineJT.UniqPolicy = Svc_Role.UniqPolicy

		WHERE (NOT(dbo.Activity.LkActivityUnsuccessfulReason LIKE 'Incorrect%'))
			AND (NOT(dbo.Activity.DescriptionOf LIKE '%Duplicate%'))
			AND (dbo.Client.LookupCode <> '')
			AND (NOT (dbo.Client.LookupCode LIKE '%TESTACCT%'))
			AND (NOT (dbo.Client.LookupCode LIKE '%PLUGACCT%'))
			AND (NOT (dbo.Client.LookupCode LIKE 'MORR&GA-01'))
			--AND ((dbo.Activity.InsertedDate BETWEEN DATEADD(MONTH,-13,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0)) AND EOMONTH(DATEADD(MONTH,-2,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0))))
			--	OR (dbo.Activity.ClosedDate BETWEEN DATEADD(MONTH,-13,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0)) AND EOMONTH(DATEADD(MONTH,-2,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0))))
			--	OR (dbo.ActivityNote.InsertedDate BETWEEN DATEADD(MONTH,-13,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0)) AND EOMONTH(DATEADD(MONTH,-2,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0))))
			--	OR (dbo.ActivityTask.InsertedDate BETWEEN DATEADD(MONTH,-13,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0)) AND EOMONTH(DATEADD(MONTH,-2,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0))))
			--	OR (dbo.Attachment.InsertedDate BETWEEN DATEADD(MONTH,-13,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0)) AND EOMONTH(DATEADD(MONTH,-2,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0)))))
	) AS Activity_Events

UNPIVOT 
	(
	Employee FOR Employees IN (Act_Ins_By, Act_Cls_By, Note_Ins_By, Task_Ins_By, Att_Ins_By)
	) AS eeup 

UNPIVOT 
	(
	ID FOR IDs IN (Act_Ins_ID, Act_Cls_ID, Note_Ins_ID, Task_Ins_ID, Att_Ins_ID)
	) AS idup 

UNPIVOT 
	(
	Event_Date FOR Event_Dates IN (Act_Ins_Date, Act_Cls_Date, Note_Ins_Date, Task_Ins_Date, Att_Ins_Date)
	) AS dateup 
	
UNPIVOT 
	(
	Event_Type FOR Event_Types IN (Act_Ins_Event, Act_Cls_Event, Note_Ins_Event, Task_Ins_Event, Att_Ins_Event)
	) AS typeup 

UNPIVOT 
	(
	Event_Desc FOR Event_Descs IN (Act_Ins_Desc, Act_Cls_Desc, Note_Ins_Desc, Task_Ins_Desc, Att_Ins_Desc)
	) AS descup
UNPIVOT 
	(
	Event_Uniq FOR Event_Uniqs IN (Act_Ins_Uniq,Act_Cls_Uniq,Note_Ins_Uniq,Task_Ins_Uniq,Att_Ins_Uniq)
	) AS uniqup

WHERE
	LEFT(Employees, 6) = LEFT(IDs, 6)
	AND LEFT(Employees, 6) = LEFT(Event_Dates, 6)
	AND LEFT(Employees, 6) = LEFT(Event_Types, 6)
	AND LEFT(Event_Types, 6) = LEFT(Event_Descs, 6)
	AND LEFT(Event_Uniqs,6) = LEFT(Event_Types,6)
	AND (Event_Date BETWEEN DATEADD(MONTH,-13,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0)) AND EOMONTH(DATEADD(MONTH,-2,DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE()),0))))
	AND (NOT([Employee] IN ('XHR001','APPLIED','APPLIEDSUPPORT','APPLIED1','APPLIED3','ZZZZAP','DOWNLOAD','ENTERPRISEADMIN','CONFIG','EPICSDKUSER','GOLIVE','SYSTEM','ZYWAE1')))

ORDER BY Event_Date