import { NextRequest,NextResponse } from "next/server";
import { unstable_rethrow } from "next/navigation";
import {getBigQueryClient} from "@/lib/bigquery";
import {prepare,round,settlementCTE,type ScenarioRequest} from "@/lib/scenarios/apiCommon";
type ResultFilter="all"|"winners"|"losers";
type RequestBody=ScenarioRequest&{page?:number;pageSize?:number;resultFilter?:ResultFilter};
type Selection={date:string;course:string|null;raceTime:string|null;raceName:string|null;horseName:string|null;horseNumber:string|null;odds:number|string;position:number|string;isWinner:boolean;isEachWayEligible:boolean|null;placesPaid:number|string|null;placeTerms:number|string|null;staked:number|string;returns:number|string;profit:number|string};
type Row={totals:{totalSelections:number|string;winners:number|string;staked:number|string;returns:number|string;profit:number|string};filteredCount:number|string;selections:Selection[]};
const bad=(error:string)=>NextResponse.json({error},{status:400});
export async function POST(request:NextRequest) {
 try {
  let body:RequestBody;
  try {body=await request.json() as RequestBody;} catch{return bad("Invalid JSON.");}
  let config:ReturnType<typeof prepare>;
  try {config=prepare(body);} catch(error){return bad((error as Error).message);}
  const {page=1,pageSize=50,resultFilter="all"}=body;
  if (!Number.isInteger(page)||page<1||page>100000) return bad("Page must be a positive integer.");
  if (!Number.isInteger(pageSize)||pageSize<1||pageSize>100) return bad("Page size must be between 1 and 100.");
  if (!["all","winners","losers"].includes(resultFilter)) return bad("Invalid result filter.");
  const {conditions,params,divisor,stakeMode,filters}=config;
  const resultCondition=resultFilter==="winners"?"is_winner":resultFilter==="losers"?"NOT is_winner":"TRUE";
  const query=`${settlementCTE(conditions,divisor,true)},
 final_selections AS (SELECT *,amount_returned-amount_staked AS profit FROM settled),
 totals AS (SELECT COUNT(*) AS totalSelections,COUNTIF(is_winner) AS winners,COALESCE(SUM(amount_staked),0) AS staked,COALESCE(SUM(amount_returned),0) AS returns,COALESCE(SUM(profit),0) AS profit FROM final_selections),
 paged AS (SELECT * FROM final_selections WHERE ${resultCondition} ORDER BY date DESC,raceTime DESC,course,horseName LIMIT @pageSize OFFSET @offset)
 SELECT (SELECT AS STRUCT totalSelections,winners,staked,returns,profit FROM totals) AS totals,
 (SELECT COUNT(*) FROM final_selections WHERE ${resultCondition}) AS filteredCount,
 ARRAY(SELECT AS STRUCT date,course,raceTime,raceName,horseName,horseNumber,odds,position,is_winner AS isWinner,isEachWayEligible,placesPaid,placeTerms,amount_staked AS staked,amount_returned AS returns,profit FROM paged) AS selections`;
  const [rows]=await getBigQueryClient().query({query,params:{...params,pageSize,offset:(page-1)*pageSize},maximumBytesBilled:"10000000000"});
  const row=(rows as Row[])[0];if(!row)throw new Error("BigQuery returned no selections response.");
  const selections=row.selections.map(s=>({...s,odds:Number(s.odds),position:Number(s.position),placesPaid:s.placesPaid===null?null:Number(s.placesPaid),placeTerms:s.placeTerms===null?null:Number(s.placeTerms),staked:round(Number(s.staked)),returns:round(Number(s.returns)),profit:round(Number(s.profit))}));
  const filteredCount=Number(row.filteredCount);
  return NextResponse.json({selections,pagination:{page,pageSize,totalItems:filteredCount,totalPages:Math.ceil(filteredCount/pageSize)},totals:{selections:Number(row.totals.totalSelections),winners:Number(row.totals.winners),staked:round(Number(row.totals.staked)),returns:round(Number(row.totals.returns)),profit:round(Number(row.totals.profit))},settings:{...body,stakeMode,filters,resultFilter}});
 }catch(error){unstable_rethrow(error);console.error("Scenario selections error:",error);return NextResponse.json({error:"Failed to retrieve scenario selections."},{status:500});}
}
