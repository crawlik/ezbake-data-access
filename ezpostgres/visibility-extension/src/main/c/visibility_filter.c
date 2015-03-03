/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Wsign-conversion"

#include "postgres.h"

#include <string.h>

#include "fmgr.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/planner.h"
#include "parser/parse_func.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

void _PG_init(void);
void _PG_fini(void);

static PlannedStmt *visibility_filter_planner(Query *parse,
									int cursorOptions,
									ParamListInfo boundParams);
static void add_filter_to_plan_recursive(Plan *plan, List *rtable);
static void add_filter_to_plan_list(List *plans, List *rtable);
static void add_filter_to_scan(Scan *scan, List *rtable);
static Var *find_visibility_column(RangeTblEntry *rte, Index scanrelid);
static FuncExpr *find_visibility_function(Var *arg);

static planner_hook_type prev_planner = NULL;

static const char * const VISIBILITY_COLUMN_NAME = "visibility";
static const char * const VISIBILITY_FUNCTION_NAME =
		"verify_row_visible_current_setting";

void
_PG_init(void)
{
	prev_planner = planner;
	planner_hook = visibility_filter_planner;
}

void
_PG_fini(void)
{
	planner_hook = prev_planner;
}

static PlannedStmt *
visibility_filter_planner(Query *parse, int cursorOptions,
		ParamListInfo boundParams)
{
	PlannedStmt *result;
	bool prev_enable_indexonlyscan;

	elog(DEBUG1, "running visibility filter planner");
	/* Index-only scans can make the visibility column unavailable to our
     * post-hoc filter. This isn't _too_ horrible since each connection gets
	 * its own fork()'d process so shouldn't be sharing the value of
	 * enable_indexonlyscan.
	 *
	 * XXX: there should be a better way to do this.
	 */
	prev_enable_indexonlyscan = enable_indexonlyscan;
	enable_indexonlyscan = false;
	result = standard_planner(parse, cursorOptions, boundParams);
	enable_indexonlyscan = prev_enable_indexonlyscan;

	add_filter_to_plan_recursive(result->planTree, result->rtable);
	add_filter_to_plan_list(result->subplans, result->rtable);

	return result;
}

static void
add_filter_to_plan_recursive(Plan *plan, List *rtable)
{
	if (plan == NULL)
	{
		return;
	}

	/* These are probably more scan types than we want to attempt to filter,
	 * but we'd rather be overly broad here than filter too little. */
	switch (plan->type)
	{
		case T_Scan:
		case T_SeqScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapIndexScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_WorkTableScan:
		case T_ForeignScan:
			add_filter_to_scan((Scan *) plan, rtable);
			break;
		case T_ModifyTable:
			add_filter_to_plan_list(((ModifyTable *) plan)->plans, rtable);
			break;
		case T_Append:
			add_filter_to_plan_list(((Append *) plan)->appendplans, rtable);
			break;
		case T_MergeAppend:
			add_filter_to_plan_list(((MergeAppend *) plan)->mergeplans, rtable);
			break;
		case T_BitmapAnd:
			add_filter_to_plan_list(((BitmapAnd *) plan)->bitmapplans, rtable);
			break;
		case T_BitmapOr:
			add_filter_to_plan_list(((BitmapOr *) plan)->bitmapplans, rtable);
			break;
		default:
			/* Do nothing */
			break;
	}

	add_filter_to_plan_recursive(plan->lefttree, rtable);
	add_filter_to_plan_recursive(plan->righttree, rtable);
}

static void
add_filter_to_plan_list(List *plans, List *rtable)
{
	ListCell *lc;

	foreach (lc, plans)
	{
		add_filter_to_plan_recursive((Plan *) lfirst(lc), rtable);
	}
}

static void
add_filter_to_scan(Scan *scan, List *rtable)
{
	RangeTblEntry *rte;
	Var *var;

	rte = (RangeTblEntry *) list_nth(rtable, scan->scanrelid - 1);
	var = find_visibility_column(rte, scan->scanrelid);

	if (var == NULL)
	{
		elog(DEBUG1, "no visibility column on %s", rte->eref->aliasname);
	}
	else
	{
		FuncExpr *expr;

		elog(DEBUG1, "adding filter to scan on %s", rte->eref->aliasname);
		expr = find_visibility_function(var);
		if (expr != NULL)
		{
			Plan *plan = (Plan *) scan;

			plan->qual = lappend(plan->qual, expr);
		}
	}
}

static Var *
find_visibility_column(RangeTblEntry *rte, Index scanrelid)
{
	ListCell *lc;
	Index currInd;
	Var *var;

	var = NULL;
	currInd = 1;
	foreach (lc, rte->eref->colnames)
	{
		Value *val = (Value *) lfirst(lc);

		if (val->type == T_String)
		{
			if (strcmp(val->val.str, VISIBILITY_COLUMN_NAME) == 0)
			{
				var = makeVar(scanrelid, /* varno */
						currInd,	/* varattrno */
						VARCHAROID, /* vartype - varchar */
						-1,		 	/* vartypmod - this shouldn't be ignored */
						InvalidOid, /* varcollid - not a collation */
						0);		 	/* varlevelsup - normal var */

				break;
			}
		}

		currInd++;
	}

	return var;
}

static FuncExpr *
find_visibility_function(Var *arg)
{
	List *funcname;
	Oid varcharOid = VARCHAROID;
	Oid funcoid;
	FuncExpr *expr;

	/* We know exactly what this function should look like */
	funcname = list_make1(makeString(pstrdup(VISIBILITY_FUNCTION_NAME)));
	funcoid = LookupFuncName(funcname, 1, &varcharOid, false);
	list_free(funcname);

	expr = NULL;
	if (funcoid == InvalidOid)
	{
		elog(WARNING, "visibility function %s not found",
				VISIBILITY_FUNCTION_NAME);
	}
	else
	{
		expr = makeFuncExpr(funcoid, BOOLOID, list_make1(arg), InvalidOid,
				InvalidOid, COERCE_EXPLICIT_CAST);
	}

	return expr;
}
