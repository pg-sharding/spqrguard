
#include "postgres.h"

#include "optimizer/planner.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "catalog/pg_class_d.h"
#include "catalog/pg_class.h"

#include "catalog/pg_namespace_d.h"
#include "catalog/pg_namespace.h"

#include "utils/fmgroids.h"
#include "utils/snapmgr.h"

#include "access/table.h"
#include "access/tableam.h"

#include "utils/guc.h"

PG_MODULE_MAGIC;


bool prevent_distributed_table_modify = false;

static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;


typedef struct spqrguard_distributedRelations {
    List *relids;
} spqrguard_distributedRelations; 


void
spqrguard_ExecutorRun(QueryDesc *queryDesc,
					 ScanDirection direction, uint64 count, bool execute_once);

/*

typedef bool (*planstate_tree_walker_callback) (struct PlanState *planstate,
												void *context);
*/

static bool spqrguard_planstate_walker(struct PlanState *planstate,
												void *context) {
    if (IsA(planstate, ModifyTableState)) {
        ModifyTableState *mts;
        Oid relid;

        spqrguard_distributedRelations *drs = context;

        mts = (ModifyTableState*) planstate;

        relid = RelationGetRelid(mts->resultRelInfo->ri_RelationDesc);

        if (!spqrguard_check_relation(drs, relid)) {
            return false;
        }

        if (prevent_distributed_table_modify)
            elog(ERROR, "unable to modify distributed relation within read-only transaction");
    }

    return false;
}


const char * spqrguard_dr_relname = "spqr_distributed_relations";
const char * spqrguard_dr_schema = "spqr_metadata";

/* It would be more handy to have FIXED-oid relations... */


static Oid SPQRGResolveMetadataSchemaOid() {
    Snapshot snap;
    Relation nsprel;
    SysScanDesc scan;
    HeapTuple tuple;
    Oid MetadataSchemaOid;

    Form_pg_namespace nsp_type;

    MetadataSchemaOid = InvalidOid;

    /* SELECT FROM pg_catalog.pg_namespace WHERE nspname = 'spqr_metadata */
    snap = RegisterSnapshot(GetTransactionSnapshot());
    /**/
    ScanKeyData skey[1];
    
    nsprel = table_open(NamespaceRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(spqrguard_dr_schema));

    scan = systable_beginscan(nsprel, ClassNameNspIndexId, true, snap, 2, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
	    nsp_type = (Form_pg_namespace) GETSTRUCT(tuple);
        MetadataSchemaOid = nsp_type->oid;
    }

    table_close(nsprel, RowExclusiveLock);
    systable_endscan(scan);
    UnregisterSnapshot(snap);

    return MetadataSchemaOid;
}

static Oid SPQRGResolveDistrRelOid(Oid MetadataSchemaOid) {
    Snapshot snap;
    Relation classrel;
    SysScanDesc scan;
    HeapTuple tuple;
    Oid DistrRelOid;

    Form_pg_class class_type;

    DistrRelOid = InvalidOid;
    
    /* SELECT FROM pg_catalog.pg_class WHERE relname = 'spqr_distributed_relations '
    * and relnamespace = $oid; */
    snap = RegisterSnapshot(GetTransactionSnapshot());
    /**/
    ScanKeyData skey[2];
    
    classrel = table_open(RelationRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(spqrguard_dr_relname));

    ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(MetadataSchemaOid));

    scan = systable_beginscan(classrel, ClassNameNspIndexId, true, snap, 2, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
	    class_type = (Form_pg_class) GETSTRUCT(tuple);
        DistrRelOid = class_type->oid;
    }

    table_close(classrel, RowExclusiveLock);
    systable_endscan(scan);
    UnregisterSnapshot(snap);

    return DistrRelOid;
}


static populate_spqrguard(spqrguard_distributedRelations *ctx) {

}

void
spqrguard_ExecutorRun(QueryDesc *queryDesc,
					 ScanDirection direction, uint64 count, bool execute_once)
{

    spqrguard_distributedRelations cxt;
    cxt.relids = NULL;

    populate_spqrguard(&cxt);

    spqrguard_planstate_walker(queryDesc->planstate, &cxt);

    (void)planstate_tree_walker(queryDesc->planstate, spqrguard_planstate_walker,
								 &cxt);

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