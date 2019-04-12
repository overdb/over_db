defmodule OverDB.MixProject do
  use Mix.Project

  def project do
    [
      app: :over_db,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp package() do
    [
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/overdb/over_db"}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:gen_stage, "~> 0.14.1"},
      {:fastglobal, "~> 1.0"},
      {:murmur, git: "https://github.com/lpgauth/murmur"}
    ]
  end
end
