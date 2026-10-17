import "./style.css";

export const metadata = {
  title: "OFL · Pesquisa",
  description: "Mercados históricos e acervo documental",
};
export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="pt-BR">
      <body>{children}</body>
    </html>
  );
}
