import { NextRequest, NextResponse } from "next/server";
import { unstable_rethrow } from "next/navigation";
import { getBigQueryClient } from "@/lib/bigquery";
import {prepare,round,settlementCTE,type ScenarioRequest} from "@/lib/scenarios/apiCommon";

type Row={date:string;bets:number|string;winners:number|string;staked:number|string;returns:number|string;profit:number|string};
export async function POST(request:NextRequest) {
 try {
  let body:ScenarioRequest;
  try {body=await request.json() as ScenarioRequest;} catch {return NextResponse.json({error:"Invalid JSON."},{status:400});}
  let config:ReturnType<typeof prepare>;
  try {config=prepare(body);} catch(error) {return NextResponse.json({error:(error as Error).message},{status:400});}
  const {conditions,params,divisor,stakeMode,filters}=config;
  const query=`${settlementCTE(conditions,divisor,false)}
 SELECT date,COUNT(*) AS bets,COUNTIF(is_winner) AS winners,
 SUM(amount_staked) AS staked,SUM(amount_returned) AS returns,
 SUM(amount_returned-amount_staked) AS profit FROM settled GROUP BY date ORDER BY date`;
  const [rows]=await getBigQueryClient().query({query,params,maximumBytesBilled:"10000000000"});
  let cumulativeProfit=0,cumulativeStaked=0,peakProfit=0,maxDrawdown=0,totalBets=0,totalWinners=0,totalStaked=0,totalReturns=0;
  const dailyPerformance=(rows as Row[]).map(row=>{
   const profit=Number(row.profit),staked=Number(row.staked),returns=Number(row.returns);
   totalBets+=Number(row.bets);totalWinners+=Number(row.winners);totalStaked+=staked;totalReturns+=returns;
   cumulativeProfit+=profit;cumulativeStaked+=staked;peakProfit=Math.max(peakProfit,cumulativeProfit);maxDrawdown=Math.max(maxDrawdown,peakProfit-cumulativeProfit);
   return {date:row.date,bets:Number(row.bets),winners:Number(row.winners),staked:round(staked),returns:round(returns),profit:round(profit),cumulativeProfit:round(cumulativeProfit),cumulativeROI:cumulativeStaked>0?round(cumulativeProfit/cumulativeStaked*100):0};
  });
  const profit=totalReturns-totalStaked;
  const summary={bets:totalBets,winners:totalWinners,strikeRate:totalBets>0?round(totalWinners/totalBets*100):0,staked:round(totalStaked),returns:round(totalReturns),profit:round(profit),roi:totalStaked>0?round(profit/totalStaked*100):0,maxDrawdown:round(maxDrawdown)};
  return NextResponse.json({summary,dailyPerformance,settings:{...body,stakeMode,filters}});
 } catch(error) {unstable_rethrow(error);console.error("Scenario calculation error:",error);return NextResponse.json({error:"Failed to calculate scenario."},{status:500});}
}
