import type { ScenarioFilters } from "./types";
import { scenarioWhere } from "./queryFilters";
export type ScenarioRequest = {
 startDate:string; endDate:string; stake:number; betType:"win"|"each_way";
 stakeAllocation:"per_horse"|"per_race"; stakeMode?:"total"|"per_leg";
 filters?:ScenarioFilters;
};
export const TABLE = "`horseracing-pacey32-github.horseraceanalytics.Scenario_App_Data_vw`";
export const round = (value:number, decimals=2) => Math.round((value+Number.EPSILON)*10**decimals)/10**decimals;
export function prepare(body: ScenarioRequest) {
 if (!body || typeof body !== "object" || Array.isArray(body)) throw new Error("Invalid request body.");
 const {startDate,endDate,stake,betType,stakeAllocation,stakeMode="total",filters={}}=body;
 const validDate=(s:unknown) => typeof s==="string" && /^\d{4}-\d{2}-\d{2}$/.test(s) && !Number.isNaN(Date.parse(s)) && new Date(s).toISOString().slice(0,10)===s;
 if (!validDate(startDate)||!validDate(endDate)||startDate>endDate) throw new Error("Invalid date range.");
 if (typeof stake!=="number"||!Number.isFinite(stake)||stake<=0||stake>100000) throw new Error("Stake must be between 0 and 100,000.");
 if (!["win","each_way"].includes(betType)) throw new Error("Invalid bet type.");
 if (!["per_horse","per_race"].includes(stakeAllocation)) throw new Error("Invalid stake allocation.");
 if (!["total","per_leg"].includes(stakeMode)) throw new Error("Invalid stake mode.");
 if (!filters||typeof filters!=="object"||Array.isArray(filters)) throw new Error("Invalid filters.");
 const {conditions,params}=scenarioWhere(filters,startDate,endDate);
 const totalStake=betType==="each_way"&&stakeMode==="per_leg"?stake*2:stake;
 params.winLegStake=betType==="each_way"?totalStake/2:totalStake;
 params.placeLegStake=betType==="each_way"?totalStake/2:0;
 const divisor=stakeAllocation==="per_race"?"qualifying_horses_in_race":"1";
 return {conditions,params,divisor,stakeMode,filters};
}
export function settlementCTE(conditions:string[],divisor:string,withDetails:boolean) {
 return `WITH filtered AS (
 SELECT RaceDateDt,Pre_RaceLocation,Pre_RaceTime,${withDetails?'Pre_RaceName,HorseName,HorseNumber,':''}OddsDecimal,Result_Position,places_paid,place_terms,ew_possible
 FROM ${TABLE} WHERE ${conditions.join(" AND ")}
 ), selections AS (
 SELECT *,COUNT(*) OVER(PARTITION BY RaceDateDt,Pre_RaceLocation,Pre_RaceTime) AS qualifying_horses_in_race FROM filtered
 ), stakes AS (
 SELECT *,SAFE_DIVIDE(@winLegStake,${divisor}) AS win_stake,SAFE_DIVIDE(@placeLegStake,${divisor}) AS place_stake FROM selections
 ), settled AS (
 SELECT CAST(RaceDateDt AS STRING) AS date,${withDetails?'Pre_RaceLocation AS course,CAST(Pre_RaceTime AS STRING) AS raceTime,Pre_RaceName AS raceName,HorseName AS horseName,HorseNumber AS horseNumber,OddsDecimal AS odds,Result_Position AS position,ew_possible AS isEachWayEligible,places_paid AS placesPaid,place_terms AS placeTerms,':''}
 Result_Position=1 AS is_winner,
 win_stake+IF(@placeLegStake>0 AND ew_possible,place_stake,0) AS amount_staked,
 IF(Result_Position=1,win_stake*OddsDecimal,0)+IF(@placeLegStake>0 AND ew_possible AND Result_Position BETWEEN 1 AND places_paid,place_stake*(1+(OddsDecimal-1)*place_terms),0) AS amount_returned
 FROM stakes
 )`;
}
