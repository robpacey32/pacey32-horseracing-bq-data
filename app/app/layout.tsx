import type { Metadata } from "next";
import {
  Cormorant_Garamond,
  Inter,
} from "next/font/google";

import "./globals.css";
import Header from "@/components/Header";

const cormorant = Cormorant_Garamond({
  variable: "--font-cormorant",
  subsets: ["latin"],
  weight: [
    "400",
    "500",
    "600",
    "700",
  ],
});

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Pacey32 Racing",
  description:
    "Pacey32 Horse Racing Analytics",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      className={`${cormorant.variable} ${inter.variable}`}
    >
      <body className="min-h-full">
        <Header />
        {children}
      </body>
    </html>
  );
}