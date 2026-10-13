export interface ExchangeRate {
  gbpToEur: number;
  eurToGbp: number;
  date: string;
  source: "ECB";
}

const ECB_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml";

export async function getExchangeRate(): Promise<ExchangeRate> {
  const response = await fetch(ECB_URL, {
    next: { revalidate: 60 * 60 * 24 },
    signal: AbortSignal.timeout(10000),
  });

  if (!response.ok) {
    throw new Error(`ECB exchange-rate request failed: ${response.status}`);
  }

  const xml = await response.text();
  const date = xml.match(/<Cube\s+time=["'](\d{4}-\d{2}-\d{2})["']/)?.[1];
  const gbpToEurString = xml.match(/<Cube\s+currency=["']GBP["']\s+rate=["']([\d.]+)["']/)?.[1];
  const eurToGbp = Number(gbpToEurString);

  if (!date || !Number.isFinite(eurToGbp) || eurToGbp <= 0) {
    throw new Error("ECB exchange-rate feed contained no valid GBP rate or publication date.");
  }

  return {
    gbpToEur: 1 / eurToGbp,
    eurToGbp,
    date,
    source: "ECB",
  };
}
