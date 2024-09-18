
#include "postgres.h"

#include "optimizer/planner.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "utils/guc.h"

PG_MODULE_MAGIC;


bool prevent_distributed_table_modify = false;

static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;

void
spqrguard_ExecutorRun(QueryDesc *queryDesc,
					 ScanDirection direction, uint64 count, bool execute_once);

/*
*/


void
spqrguard_ExecutorRun(QueryDesc *queryDesc,
					 ScanDirection direction, uint64 count, bool execute_once)
{

    if (prevent_distributed_table_modify) {
        if (IsA(queryDesc->planstate, ModifyTableState)) {
            elog(ERROR, "unable to modify distributed relation within read-only transaction");
        }
    }

    (void) standard_ExecutorRun(queryDesc, direction, count, execute_once);
}



void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "This module can only be loaded via shared_preload_libraries");
		return;
	}

    DefineCustomBoolVariable("spqrguard.prevent_distributed_table_modify",
                            "Restrict sql referencing one of SPQR distributed relations to be read-only",
                            "Default of false",
                            &prevent_distributed_table_modify,
                            false,
                            PGC_SUSET,
                            GUC_NOT_IN_SAMPLE,
                            NULL,
                            NULL,
                            NULL);

#if PG_VERSION_NUM >= 130000
	prev_ExecutorRun_hook = ExecutorRun_hook;
	ExecutorRun_hook = spqrguard_ExecutorRun;
#endif

}