#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>
#include <catalog/pg_collation.h>
#include <utils/tuplesort.h>
#include <utils/typcache.h>
#include <utils/builtins.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

struct median {
	uint32 num_elems;
	Tuplesortstate *tss;
};

static void initialize_state(MemoryContext agg_context, bytea **state)
{
	Size len = VARHDRSZ + sizeof(struct median);

	*state = MemoryContextAllocZero(agg_context, len);
	if (!*state)
		elog(ERROR, "could not initialize state");

	SET_VARSIZE(*state, len);
}

static Tuplesortstate *initialize_tuplesort(FmgrInfo *finfo)
{
	Tuplesortstate *tss;
	TypeCacheEntry *typc;
	Oid collation = InvalidOid,
		oid = get_fn_expr_argtype(finfo, 1);

	if (oid == InvalidOid)
		return NULL;

	typc = lookup_type_cache(oid, TYPECACHE_LT_OPR);
	if (!typc)
		elog(ERROR, "could not get type cache entry for type %u", oid);

	if (typc->lt_opr == InvalidOid)
		elog(ERROR, "could not get less-than operator for type %u", oid);

	if (oid == TEXTOID)
		collation = C_COLLATION_OID;

	tss = tuplesort_begin_datum(
			oid,		/* datum type */
			typc->lt_opr,	/* less-than operator */
			collation,
			0,
			5000,
			0);		/* no random access */
	return tss;
}

PG_FUNCTION_INFO_V1(median_transfn);

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */
Datum
median_transfn(PG_FUNCTION_ARGS)
{
	Datum val;
	struct median *ms;
	MemoryContext agg_context;
	bytea *state = (PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_transfn called in non-aggregate context");
	if (PG_NARGS() < 2)
		elog(ERROR, "too few arguments");

	if (!state)
	{
		initialize_state(agg_context, &state);

		ms = (struct median *) VARDATA(state);
		ms->tss = initialize_tuplesort(fcinfo->flinfo);
		if (!ms->tss)
			elog(ERROR, "could not initialize tuplesort");
	}

	if (!PG_ARGISNULL(1))	/* skip null values */
	{
		val = PG_GETARG_DATUM(1);
		ms = (struct median *) VARDATA(state);

		tuplesort_putdatum(ms->tss, val, 0);
		ms->num_elems++;
	}

	PG_RETURN_BYTEA_P(state);
}

PG_FUNCTION_INFO_V1(median_finalfn);

/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 */
Datum
median_finalfn(PG_FUNCTION_ARGS)
{
	Datum val, ab;
	bool is_null;
	uint32 pos;
	struct median *ms;
	MemoryContext agg_context;
	bytea *state = (PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_P(0));

	if (!AggCheckCallContext(fcinfo, &agg_context))
		elog(ERROR, "median_finalfn called in non-aggregate context");
	if (PG_NARGS() < 2)
		elog(ERROR, "too few arguments");

	if (!state)
		PG_RETURN_NULL();

	ms = (struct median *) VARDATA(state);

	tuplesort_performsort(ms->tss);

	pos = ms->num_elems / 2;

	if (!tuplesort_skiptuples(ms->tss, pos, 1))
		elog(ERROR, "could not advance %u slots", pos);
	if (!tuplesort_getdatum(ms->tss, 1, &val, &is_null, &ab))
		elog(ERROR, "could not get element after advancing %u slots", pos);

//	tuplesort_end(ms->tss);

	if (is_null)
		elog(ERROR, "element at position %u is null - this should not happen", pos);

	PG_RETURN_DATUM(val);
}
