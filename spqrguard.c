
#include "postgres.h"

#include "c.h"

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
#if PG_VERSION_NUM < 140000
#include "catalog/indexing.h"
#endif

#include "utils/fmgroids.h"
#include "utils/snapmgr.h"

#include "access/table.h"
#include "access/tableam.h"
#include "access/genam.h"

#include "access/genam.h"

#include "fmgr.h"

#include "utils/builtins.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;


bool prevent_distributed_table_modify = false;
bool prevent_reference_table_modify = false;

static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;


typedef struct spqrguard_distributedRelations {
    Oid spqr_d_metadata_reloid;
    Oid spqr_ref_metadata_reloid;

    Oid spqr_global_settings_reloid;

    bool initialized;

    bool prevent_distributed_table_modify;
    bool prevent_reference_table_modify;
} spqrguard_distributedRelations; 


void
spqrguard_ExecutorRun(QueryDesc *queryDesc,
					 ScanDirection direction, uint64 count, bool execute_once);


static bool spqrguard_check_relation(spqrguard_distributedRelations *ctx, Oid relid) {
    /* NOOP for now */
    Relation spqrrel;
    SysScanDesc scan;
    HeapTuple tuple;
    bool    res;
    ScanKeyData skey[1];

    res = false;

    /* SELECT FROM pg_catalog.pg_namespace WHERE nspname = 'spqr_metadata */
    /**/
    if (!ctx->initialized)
    {
	    res = true;
	    return res;
    }
    spqrrel = table_open(ctx->spqr_d_metadata_reloid, AccessShareLock);

#define Anum_spqr_distributed_relations_reloid 1

    ScanKeyInit(&skey[0], Anum_spqr_distributed_relations_reloid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relid));

    scan = systable_beginscan(spqrrel, InvalidOid, false, NULL, 1, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
        res = true;
    }

    table_close(spqrrel, AccessShareLock);
    systable_endscan(scan);

    return res;
}

static bool spqrguard_check_ref_relation(spqrguard_distributedRelations *ctx, Oid relid) {
    /* NOOP for now */
    Relation spqrrel;
    SysScanDesc scan;
    HeapTuple tuple;
    bool    res;
    ScanKeyData skey[1];

    res = false;

    /* SELECT FROM pg_catalog.pg_namespace WHERE nspname = 'spqr_metadata */
    /**/
    if (!ctx->initialized)
    {
	    res = true;
	    return res;
    }
    spqrrel = table_open(ctx->spqr_ref_metadata_reloid, AccessShareLock);

#define Anum_spqr_reference_relations_reloid 1

    ScanKeyInit(&skey[0], Anum_spqr_reference_relations_reloid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relid));

    scan = systable_beginscan(spqrrel, InvalidOid, false, NULL, 1, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
        res = true;
    }

    table_close(spqrrel, AccessShareLock);
    systable_endscan(scan);

    return res;
}

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

        if (spqrguard_check_relation(drs, relid)) {
            if (drs->prevent_distributed_table_modify)
                elog(ERROR, "unable to modify SPQR distributed relation within read-only transaction");
        }
        
        if (spqrguard_check_ref_relation(drs, relid)) {
            if (drs->prevent_reference_table_modify)
                elog(ERROR, "unable to modify SPQR reference relation within read-only transaction");
        }
    }

    return false;
}


const char * spqrguard_dr_relname = "spqr_distributed_relations";
const char * spqrguard_dr_schema = "spqr_metadata";
const char * spqrguard_global_settings = "spqr_global_settings";

/* It would be more handy to have FIXED-oid relations... */


static Oid SPQRGResolveMetadataSchemaOid() {
    Relation nsprel;
    SysScanDesc scan;
    HeapTuple tuple;
    Oid MetadataSchemaOid;
    ScanKeyData skey[1];
    Form_pg_namespace nsp_type;

    MetadataSchemaOid = InvalidOid;

    /* SELECT FROM pg_catalog.pg_namespace WHERE nspname = 'spqr_metadata */
    /**/
    
    nsprel = table_open(NamespaceRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_namespace_nspname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(spqrguard_dr_schema));

    scan = systable_beginscan(nsprel, NamespaceNameIndexId, true, NULL, 1, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
	    nsp_type = (Form_pg_namespace) GETSTRUCT(tuple);
        MetadataSchemaOid = nsp_type->oid;
    }

    table_close(nsprel, RowExclusiveLock);
    systable_endscan(scan);

    return MetadataSchemaOid;
}

static Oid SPQRGResolveDistrRelOid(Oid MetadataSchemaOid) {
    Relation classrel;
    SysScanDesc scan;
    HeapTuple tuple;
    Oid DistrRelOid;
    ScanKeyData skey[2];
    Form_pg_class class_type;

    DistrRelOid = InvalidOid;
    
    /* SELECT FROM pg_catalog.pg_class WHERE relname = 'spqr_distributed_relations '
    * and relnamespace = $oid; */
    /**/
    
    classrel = table_open(RelationRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(spqrguard_dr_relname));

    ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(MetadataSchemaOid));

    scan = systable_beginscan(classrel, ClassNameNspIndexId, true, NULL, 2, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
	    class_type = (Form_pg_class) GETSTRUCT(tuple);
        DistrRelOid = class_type->oid;
    }

    table_close(classrel, RowExclusiveLock);
    systable_endscan(scan);

    return DistrRelOid;
}

static Oid SPQRGResolveGlobalSettingsOid(Oid MetadataSchemaOid) {
    Relation classrel;
    SysScanDesc scan;
    HeapTuple tuple;
    Oid SetRelOid;
    ScanKeyData skey[2];
    Form_pg_class class_type;

    SetRelOid = InvalidOid;
    
    /* SELECT FROM pg_catalog.pg_class WHERE relname = 'spqr_distributed_relations '
    * and relnamespace = $oid; */
    /**/
    
    classrel = table_open(RelationRelationId, RowExclusiveLock);

    ScanKeyInit(&skey[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(spqrguard_global_settings));

    ScanKeyInit(&skey[1], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(MetadataSchemaOid));

    scan = systable_beginscan(classrel, ClassNameNspIndexId, true, NULL, 2, skey);
    
    tuple = systable_getnext(scan);

    /* No map relation created. return invalid oid */
    if (HeapTupleIsValid(tuple)) {
	    class_type = (Form_pg_class) GETSTRUCT(tuple);
        SetRelOid = class_type->oid;
    }

    table_close(classrel, RowExclusiveLock);
    systable_endscan(scan);

    return SetRelOid;
}

typedef struct Form_DataGlobalSettings {
    int32_t name;

    text value;
} Form_DataGlobalSettings;

typedef Form_DataGlobalSettings *Form_GlobalSettings;

#define Anum_spqr_global_settings_name 1
#define Anum_spqr_global_settings_value 2

#define PREVENT_DISTRIBUTED_TABLE_MODIFY 42
#define PREVENT_REFERENCE_TABLE_MODIFY 69

static bool ResolveGlobalBoolSetting(Oid setReloid, int32_t setname) {
    Relation setrel;
#define ResolveGlobalBoolSetCols 1
    ScanKeyData skey[ResolveGlobalBoolSetCols];
    TableScanDesc desc;
    TupleTableSlot *slot;
    bool val;
    
    setrel = table_open(setReloid, AccessShareLock);
    /* default */
    val = false;
    
    ScanKeyInit(&skey[0], Anum_spqr_global_settings_name,
              BTEqualStrategyNumber, F_INT4EQ,
              Int32GetDatum(setname));
              
    desc = table_beginscan(setrel, SnapshotSelf, ResolveGlobalBoolSetCols, skey);
    
    slot = table_slot_create(setrel, NULL);

    if (table_scan_getnextslot(desc, ForwardScanDirection, slot)) {

        HeapTuple tuple;
        char *raw_value;
        text tval;
        tuple = ExecFetchSlotHeapTuple(slot, false, NULL);

        tval = ((Form_GlobalSettings) GETSTRUCT(tuple))->value;

	    raw_value = text_to_cstring(&tval);

        if (strcmp(raw_value, "ok") == 0 || 
            strcmp(raw_value, "yes") == 0 || 
            strcmp(raw_value, "true") == 0 ||
            strcmp(raw_value, "on") == 0) {
            val = true;
        }
    }

    ExecDropSingleTupleTableSlot(slot);

    table_endscan(desc);
    table_close(setrel, AccessShareLock);

    return val;
}

static void populate_spqrguard(spqrguard_distributedRelations *ctx) {
    if (ctx->spqr_d_metadata_reloid == InvalidOid)
    {
        Oid spqrguard_dr_schema_oid = SPQRGResolveMetadataSchemaOid();
        if (spqrguard_dr_schema_oid == InvalidOid) {
            /* extension not created yet, but hook is already in-place. */
            ctx->initialized = false;
        } else {
            ctx->initialized = true;
            ctx->spqr_d_metadata_reloid = SPQRGResolveDistrRelOid(spqrguard_dr_schema_oid);
            ctx->spqr_ref_metadata_reloid = SPQRGResolveDistrRelOid(spqrguard_dr_schema_oid);
            ctx->spqr_global_settings_reloid = SPQRGResolveGlobalSettingsOid(spqrguard_dr_schema_oid);
        }
    }

    ctx->prevent_distributed_table_modify = prevent_distributed_table_modify;
    ctx->prevent_reference_table_modify = prevent_reference_table_modify;

    if (ctx->spqr_global_settings_reloid == InvalidOid) {
        ctx->prevent_distributed_table_modify = false;
        ctx->prevent_reference_table_modify = false;
    } else {
        ctx->prevent_distributed_table_modify = 
            ResolveGlobalBoolSetting(ctx->spqr_global_settings_reloid, PREVENT_DISTRIBUTED_TABLE_MODIFY);
        ctx->prevent_reference_table_modify = 
            ResolveGlobalBoolSetting(ctx->spqr_global_settings_reloid, PREVENT_REFERENCE_TABLE_MODIFY);
    }
}

static spqrguard_distributedRelations cxt;

void
spqrguard_ExecutorRun(QueryDesc *queryDesc,
					 ScanDirection direction, uint64 count, bool execute_once)
{
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

    DefineCustomBoolVariable("spqrguard.prevent_reference_table_modify",
                            "Restrict sql referencing one of SPQR reference relations to be read-only",
                            "Default of false",
                            &prevent_reference_table_modify,
                            false,
                            PGC_SUSET,
                            GUC_NOT_IN_SAMPLE,
                            NULL,
                            NULL,
                            NULL);

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
