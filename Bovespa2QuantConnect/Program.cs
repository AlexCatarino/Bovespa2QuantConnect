using System;
using System.IO.Compression;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Globalization;
using System.Net.Http;
using System.Collections.Concurrent;

namespace Bovespa2QuantConnect
{
    class Program
    {
        static readonly CultureInfo _langbr = CultureInfo.CreateSpecificCulture("pt-BR");
        static readonly CultureInfo _langus = CultureInfo.CreateSpecificCulture("en-US");
        static string _rawdatafolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
        static string _leanequityfolder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
        #region TickChange Dictionary
        static Dictionary<string, string> TickerChange = new Dictionary<string, string>() 
            {   // NEW      OLD 
                {"ABCB4", "ABCB11"}, //
                {"ABEV3", "AMBV3" }, // AMBV3 AMBV4
                {"ABRE3", "ABRE11"}, //
                {"AEDU3", "AEDU11"}, //
                {"AGEI3", "AGIN3" }, // ABYA3 KSSA3
                {"AMAR3", "MARI3" }, //
                {"ARCE3", "BELG3" }, //
                {"ARTR3", "OHLB3" }, //
                {"BHGR3", "IVTT3" }, //
                {"BICB4", "BICB11"}, //
                {"BPAN4", "BPNM4" }, //
                {"BRFS3", "PRGA3" }, //
                {"BRKM3", "CPNE3" },
                {"BRKM5", "CPNE5" }, //
                {"BRKM6", "CPNE6" }, //
                {"BRSR6", "BRSR11"}, //
                {"BRTO3", "TEPR3" }, 
                {"BRTO4", "TEPR4" }, //
                {"BRTP3", "TCSP3" }, //
                {"BRTP4", "TCSP4" }, //
                {"BTOW3", "SUBA3" }, //
                {"BVMF3", "BMEF3" }, // BOVH3
                {"CESP5", "CESP4" }, //
                {"CGAS5", "CGAS4" }, //
                {"CIEL3", "VNET3" }, //
                {"CLSC4", "CLSC6" }, //
                {"DAGB11", "DUFB11"}, //
                {"DTEX3", "DURA3" }, // DURA4 SATI3
                {"EMBR4", "EMBR5" }, //
                {"ENEV3", "MPXE3" }, //
                {"ENGI3", "FLCL3" }, //
                {"ENGI4", "FLCL5" }, //
                {"EQTL3", "EQTL11"}, //
                {"ESTC3", "ESTC11"}, //
                {"FRAS3", "STED3" },
                {"FRAS4", "STED4" },
                {"FIBR3", "VCPA3" }, //
                {"GGBR3", "COGU3" }, 
                {"GGBR4", "COGU4" }, //
                {"HGCR11", "CSBC11"}, //
                {"IBAN5", "IBAN4" },
                {"ITUB3", "ITAU3" }, //
                {"ITUB4", "ITAU4" }, //
                {"KLBN3", "KLAB3" }, 
                {"KLBN4", "KLAB4" }, //
                {"LIGT3", "LIGH3" }, //
                {"MAGG3", "MAGS3" }, // MAGS5 MAGS7
                {"NETC3", "PLIM3" }, 
                {"NETC4", "PLIM4" }, //
                {"OIBR3", "BRTO3" }, //
                {"OIBR4", "BRTO4" }, //
                {"PALF5", "PALF4" },
                {"PLAS4", "OSAO4" }, //
                {"PLAS3", "PLAS4" }, //
                {"PMET6", "BCAL6" },
                {"PRBC4", "PRBC11"}, //
                {"PRIO3", "HRTP3" },
                {"PRML3", "LLXL3" },
                {"RADL3", "DROG3" }, // RAIA3
                {"REDE3", "ELCA3" },
                {"REDE4", "ELCA4" },
                {"RUMO3", "ALLL3" },
                {"SANB3", "BESP3" }, //
                {"SANB4", "BESP4" }, //
                {"SDIA3", "SOES3" }, //
                {"SDIA4", "SOES4" }, //
                {"STBP11", "STBR11"}, //
                {"SUZB5", "SUZA4" }, // BSUL
                {"SWET3", "AORE3" },
                {"TBLE3", "GRSU3" }, //
                {"TBLE5", "GRSU5" },
                {"TBLE6", "GRSU6" }, //
                {"TERI3", "ACGU3" }, //
                {"TIMP3", "TCSL3" }, // TCSL4
                {"TMAR3", "TERJ3" },
                {"TMAR6", "TERJ4" }, //
                {"VAGR3", "ECOD3" }, //
                {"VALE5", "VALE4" }, //
                {"VCPA4", "PSIM4" }, //
                {"VIVO3", "TSPP3" }, //
                {"VIVO4", "TSPP4" }, //
                {"VIVR3", "INPR3" }, //
                {"VIVT3", "TLPP3" }, //
                {"VIVT4", "TLPP4" }, //
                {"VLID3", "ABNB3" }, //
                {"WEGE3", "ELMJ3" },
                {"WEGE4", "ELMJ4" }, // 
            };
        #endregion

        static void Main(string[] args)
        {
            char key;
            var menu =
                "1. Extract raw data from COTAHIST file to QC daily cvs file.\n" +
                "2. Extract raw data from NEG* files to QC tick cvs file.\n" +
                "3. Make QC minute cvs files from QC tick cvs file.\n" +
                "4. Write QC factor files.\n" +
                //"5. NotImplemented.\n" +
                //"6. NotImplemented.\n" +
                //"7. NotImplemented.\n" +
                "8. Write QC holiday file.\n" +
                "9. Zip all raw data\n" +
                "0. Sair\n" +
                ">> Insira opção: ";
            Console.Write(menu);
            
            _rawdatafolder += @"\IBOV\Stock\";
            _leanequityfolder += @"\GitHub\Lean\Data\equity\bra\";

            do
            {
                key = Console.ReadKey().KeyChar;
                
                switch (key)
                {
                    case '1':
                        WriteQuantConnectDailyFile();
                        break;
                    case '2':
                        WriteQuantConnectTickFile();
                        break;
                    case '3':
                        WriteQuantConnectMinuteFile(1);
                        break;
                    case '4':
                        WriteQuantConnectFactorFiles();
                        break;
                    case '5':
                        WriteNelogicaFiles(true);
                        break;
                    case '6':
                        //new string[] 
                        //{ 
                        //    //"ITUB4", "GGBR4", "CYRE3", "KROT3", "HGTX3", "MRFG3",  
                        //    "BBAS3"//,"JBSS3",  "POMO4", "USIM5", "ALLL3",
                        //}.ToList().ForEach(s => AdjustedPrice(s, new DateTime(2014, 5, 31)));
                        break;
                    case '7':
                        //SearchTickerChange();
                        MergeFiles("daily");
                        break;
                    case '8':
                        WriteQuantConnectHolidayFile();
                        break;
                    case '9':
                        ZipALLRaw("daily");
                        break;
                    default:
                        Console.WriteLine("\nOpção Inválida!\n" + menu);
                        break;
                }

            } while (key != '0');
        }

        private static async Task WriteQuantConnectHolidayFile()
        {
            // America/Sao_Paulo,bra,,equity,-,-,-,-,9.5,10,16.9166667,18,9.5,10,16.9166667,18,9.5,10,16.9166667,18,9.5,10,16.9166667,18,9.5,10,16.9166667,18,-,-,-,-
            var holidays = new List<DateTime>();

            var ofile = new FileInfo(_leanequityfolder.Replace("equity\\bra", "market-hours") + "holidays-bra.csv");
            var output = ofile.Exists
                ? File.ReadAllLines(ofile.FullName).ToList()
                : (new string[] { "year, month, day" }).ToList();
            var lastdate = output.Count == 1
                ? new DateTime(1997, 12, 31)
                : DateTime.ParseExact(output.Last(), "yyyy, MM, dd", _langus);

            if (lastdate == new DateTime(DateTime.Now.Year, 12, 31)) return;

            #region Get Holidays from Bovespa page
            try
            {
                using (var client = new HttpClient())
                using (var response = await client.GetAsync("http://www.bmfbovespa.com.br/pt-br/regulacao/calendario-do-mercado/calendario-do-mercado.aspx"))
                using (var content = response.Content)
                {
                    var i = 0;
                    var id = 0;
                    var date = new DateTime();
                    var page = await content.ReadAsStringAsync();
                    page = page.Substring(0, page.IndexOf("linhaDivMais"));

                    var months = new string[] { 
                        "Jan", "Fev", "Mar",
                        "Abr", "Mai", "Jun",
                        "Jul", "Ago", "Set",
                        "Out", "Nov", "Dez" }.ToList();

                    while (i < 12)
                    {
                        while (i < 12 && (id = page.IndexOf(">" + months[i + 0] + "<")) < 0) i++;
                        var start = id + 1;

                        while (i < 11 && (id = page.IndexOf(">" + months[i + 1] + "<")) < 0) i++;
                        var count = id - start;

                        months[i] = count > 0 ? page.Substring(start, count) : page.Substring(start);

                        id = 0;
                        while ((id = months[i].IndexOf("img/ic_", id) + 6) > 6)
                        {
                            id++;
                            if (DateTime.TryParseExact(months[i].Substring(id, 2) + months[i].Substring(0, 3) + DateTime.Now.Year.ToString(),
                                "ddMMMyyyy", _langbr, DateTimeStyles.None, out date))
                                holidays.Add(date);
                        }
                        i++;
                    }
                }
            }
            catch (Exception e) { Console.WriteLine(e.Message); }
            #endregion

            while (lastdate < holidays.First()) holidays.Add(lastdate = lastdate.AddDays(1));
            holidays.RemoveAll(d => d.DayOfWeek == DayOfWeek.Saturday || d.DayOfWeek == DayOfWeek.Sunday);

            var zipfiles = new DirectoryInfo(_rawdatafolder).GetFiles("COTAHIST_A*zip").Where(zf =>
            {
                var zfyear = int.Parse(zf.Name.Substring(zf.Name.Length - 8, 4));
                return zfyear >= holidays.Min().Year && zfyear < holidays.Max().Year;
            }).ToArray();

            foreach (var zf in zipfiles)
            {
                var data = await ReadZipFile(zf);

                // Remove header and footer
                data.RemoveAt(0);
                data.RemoveAt(data.Count - 1);

                data.Select(l => DateTime.ParseExact(l.Substring(2, 8), "yyyyMMdd", _langus))
                    .ToList().ForEach(d => holidays.Remove(d));
                Console.Write("\r\n" + zf.Name + "\t" + holidays.Count);
            }

            output.AddRange(holidays.OrderBy(d => d).Select(d => d.ToString("yyyy, MM, dd")));
            File.WriteAllText(ofile.FullName, string.Join("\r\n", output.ToArray()));
            Console.WriteLine("\r\n" + ofile.Name + " written!");
        }
        
        private static async Task WriteQuantConnectDailyFile()
        {
            FolderCleanUp("daily");
                        
            var outputdir = Directory.CreateDirectory(_leanequityfolder + @"daily\");
            
            var zipfiles = new DirectoryInfo(_rawdatafolder).GetFiles("COTAHIST_A*zip")
                .Where(f => int.Parse(f.Name.Substring(f.Name.Length - 8, 4)) > 1997).ToList();

            Console.WriteLine("\r\n" + zipfiles.Count + " zip files with raw data."); 

            foreach (var zipfile in zipfiles)
            {
                var data = await ReadZipFile(zipfile);
                data.RemoveAll(d => Filter(d));

                data.GroupBy(d => d.Substring(12, 12).Trim().ToLower() + ".csv").ToList().ForEach(d =>
                {
                    File.AppendAllLines(outputdir.FullName + d.Key, d.Select(l =>
                    {
                        return l.Substring(2, 8) + "," +
                            100 * Convert.ToInt64(l.Substring(56, 13)) + "," +
                            100 * Convert.ToInt64(l.Substring(69, 13)) + "," +
                            100 * Convert.ToInt64(l.Substring(82, 13)) + "," +
                            100 * Convert.ToInt64(l.Substring(108, 13)) + "," +
                            100 * Convert.ToInt64(l.Substring(152, 18));
                    }).OrderBy(l => l));
                });
                Console.WriteLine(zipfile.Name.ToUpper() + " read\t" + data.Count);
            }

            outputdir.GetFiles("*.csv").ToList().ForEach(csvFile =>
            {
                using (var z = new FileStream(csvFile.FullName.Replace(".csv", ".zip"), FileMode.Create))
                using (var a = new ZipArchive(z, ZipArchiveMode.Create))
                    a.CreateEntryFromFile(csvFile.FullName, csvFile.Name);

                csvFile.Delete();
            });

            Console.WriteLine("Done!");
        }
               
        private static async Task WriteQuantConnectTickFile()
        {
            FolderCleanUp("tick");

            var selected = new string[] { "bbas3", "mrfg3", "petr4", "usim5", "vale5" }.ToList();
                
            var rootdir = Directory.CreateDirectory(_leanequityfolder + @"tick\");
            
            var subdirs = new Dictionary<string, string>();
            selected.ForEach(s => subdirs.Add(s, Directory.CreateDirectory(rootdir.FullName + s + @"\").FullName));

            var zipfiles = new DirectoryInfo(_rawdatafolder).GetFiles("NEG*zip").ToList()
                .FindAll(f => DateTime.ParseExact(f.Name.Substring(4, 8),"yyyyMMdd",_langus) >= new DateTime(2011,10,1));

            Console.WriteLine("\r\n" + zipfiles.Count + " zip files with raw data."); 

            foreach (var zipfile in zipfiles)
            {
                var starttime = DateTime.Now;

                using (var zip2open = new FileStream(zipfile.FullName, FileMode.Open, FileAccess.Read))
                using (var archive = new ZipArchive(zip2open, ZipArchiveMode.Read))
                    foreach (var entry in archive.Entries)
                        using (var file = new StreamReader(entry.Open()))
                        {
                            var lastdate = "20050101";
                            var output = new List<string[]>();
                            
                            while (!file.EndOfStream)
                            {
                                var csv = (await file.ReadLineAsync()).Split(';');
                                if (csv.Length < 5 || !selected.Contains(csv[1] = csv[1].Trim().ToLower())) continue;
                                csv[0] = csv[0].Replace("-", "");
                                csv[2] = TimeSpan.Parse(csv[5], _langus).TotalMilliseconds.ToString("0.000", _langus) + "," +
                                    (10000 * decimal.Parse(csv[3], _langus)).ToString("0") + "," + Convert.ToInt64(csv[4]);

                                if (output.Count == 0) lastdate = csv[0];
                                if (csv[0] == lastdate) { output.Add(csv); continue; }
                                
                                // Write QuantConnect zip file
                                output.GroupBy(o => o[1]).ToList().ForEach(s =>
                                {
                                    var csvFile = new FileInfo(lastdate + "_" + s.Key + "_Trade_Tick.csv");
                                    File.WriteAllLines(csvFile.FullName, s.Select(d => d[2]));

                                    var newFile = subdirs[s.Key] + lastdate + "_trade.zip";
                                    using (var z = new FileStream(newFile, FileMode.Create))
                                    using (var a = new ZipArchive(z, ZipArchiveMode.Create))
                                        a.CreateEntryFromFile(csvFile.FullName, csvFile.Name);

                                    csvFile.Delete();
                                });
                                output.Clear();
                            }
                        }

                Console.WriteLine((((1 + zipfiles.IndexOf(zipfile)) / (double)zipfiles.Count)).ToString("0.00%\t", _langus) +
                    zipfile.Name.ToUpper() + " read in " + (DateTime.Now - starttime).ToString(@"mm\:ss"));
            }
        }
       
        private static async Task WriteQuantConnectFactorFiles()
        {
            FolderCleanUp("factor_files");
            
            var codes = new List<int>();
            var symbols = new List<string>();
            var alphabet = new List<char>();
            var startdate = new DateTime(1998, 1, 1);
            alphabet.Clear();

            #region GetSymbols
            new DirectoryInfo(_leanequityfolder + @"daily\").GetFiles("*.zip").ToList().ForEach(z =>
            {
                var symbol = z.Name.Replace(".zip", "").ToUpper();
                if (!alphabet.Contains(symbol[0])) alphabet.Add(symbol[0]);
                if (symbol.Length > 3 && !symbols.Contains(symbol)) symbols.Add(symbol);
            });
            #endregion

            #region GetCodes
            if (File.Exists("exception.txt"))
            {
                File.ReadAllLines("exception.txt").ToList().ForEach(l =>
                    {
                        int code;
                        if (int.TryParse(l.Split(';')[0], out code) && code > 0 && !codes.Contains(code)) codes.Add(code);
                    });
                File.Delete("exception.txt");
                alphabet.Clear();
            }
            foreach (var letter in alphabet)
            {
                var url = "http://www.bmfbovespa.com.br/cias-listadas/empresas-listadas/BuscaEmpresaListada.aspx?Letra=";

                try
                {
                    using (var client = new HttpClient())
                    using (var response = await client.GetAsync(url + letter))
                    using (var content = response.Content)
                    {
                        var id = 0;
                        var page = await content.ReadAsStringAsync();
                        Console.Write(letter);

                        while ((id = page.IndexOf("codigoCvm", id) + 9) > 9)
                        {
                            id++;
                            var code = 0;
                            if (!int.TryParse(page.Substring(id, 5), out code) &&
                                !int.TryParse(page.Substring(id, 4), out code) &&
                                !int.TryParse(page.Substring(id, 3), out code) &&
                                !int.TryParse(page.Substring(id, 2), out code))
                                Console.WriteLine("");

                            if (!codes.Contains(code)) codes.Add(code);
                        }
                    }

                }
                catch (Exception e)
                {
                    //File.AppendAllText(errtxt, letter + ";");
                    Console.WriteLine(e.Message);
                }
            }
            if (codes.Contains(23264)) codes.Add(18112);
            codes = codes.OrderBy(i => i).ToList();
            #endregion

            if (!codes.Contains(18112)) codes.Add(18112);
            //if (!codes.Contains(4170)) codes.Add(4170); // VALE
            //if (!codes.Contains(9512)) codes.Add(9512); // PETR

            #region Get Ticker Merge
            var mergefile = new FileInfo("MergeEvent.txt");
            if (mergefile.Exists)
            { 
                
            }
            #endregion

            foreach (var code in codes)
            {
                var index = 0;
                var page0 = string.Empty;
                var page1 = string.Empty;
                var page2 = string.Empty;

                #region Read pages
                try
                {
                    using (var client = new HttpClient())
                    {
                        var url0 = "http://www.bmfbovespa.com.br/pt-br/mercados/acoes/empresas/ExecutaAcaoConsultaInfoEmp.asp?CodCVM=" + code;
                        var url1 = "http://www.bmfbovespa.com.br/Cias-Listadas/Empresas-Listadas/ResumoProventosDinheiro.aspx?codigoCvm=" + code;
                        var url2 = url1.Replace("ProventosDinheiro", "EventosCorporativos");

                        using (var response = await client.GetAsync(url0))
                        using (var content = response.Content) page0 = await content.ReadAsStringAsync();

                        using (var response = await client.GetAsync(url1))
                        using (var content = response.Content) page1 = await content.ReadAsStringAsync();

                        using (var response = await client.GetAsync(url2))
                        using (var content = response.Content) page2 = await content.ReadAsStringAsync();
                    }
                }
                catch (Exception e) { Console.WriteLine(e.Message); }

                #endregion

                if ((index = page0.IndexOf("Papel=") + 6) < 6) continue;
                var length = page0.IndexOf("&", index) - index;
                page0 = page0.Substring(index, length);

                if (!page1.Contains("Proventos em Dinheiro")) page1 = string.Empty;
                if ((index = page1.IndexOf("<tbody>")) >= 0) page1 = page1.Substring(0, page1.IndexOf("</tbody>")).Substring(index);

                if (!page2.Contains("Proventos em Ações")) page2 = string.Empty;
                if ((index = page2.IndexOf("<tbody>")) >= 0) page2 = page2.Substring(0, page2.IndexOf("</tbody>")).Substring(index);

                var kind = new Dictionary<int, string> { { 3, "ON" }, { 4, "PN" }, { 5, "PNA" }, { 6, "PNB" }, { 7, "PNC" }, { 8, "PND" }, { 11, "UNT" } };
                var thiscodesymbols = symbols.Intersect(page0.Split(',')).ToList();
                if (thiscodesymbols.Count == 0) continue;

                foreach (var symbol in thiscodesymbols)
                {
                    var date = new DateTime();
                    var keys = new List<DateTime>();
                    var fkeys = new List<DateTime>();

                    var events = new Dictionary<DateTime, decimal>();
                    var factors = new Dictionary<DateTime, decimal>();

                    var dividend = new Dictionary<DateTime, decimal>();
                    var comprice = new Dictionary<DateTime, decimal>();

                    #region Dividends
                    try
                    {
                        index = 0;

                        while (page1.Length > 0 && (index = page1.IndexOf(">" + kind[int.Parse(symbol.Substring(4))] + "<", index)) > 0)
                        {
                            index++;
                            var idx = 0;
                            var cols = new List<string>();
                            var row = page1.Substring(index, page1.IndexOf("</tr>", index) - index);

                            while ((idx = row.IndexOf("\">", idx) + 2) >= 2) cols.Add(row.Substring(idx, row.IndexOf("<", idx) - idx));

                            var currentcomprice = 0m;
                            if (!decimal.TryParse(cols[5], NumberStyles.Any, _langbr, out currentcomprice) || currentcomprice <= 0) continue;

                            if (!DateTime.TryParseExact(cols[4], "dd/MM/yyyy", _langbr, DateTimeStyles.None, out date) &&
                                !DateTime.TryParseExact(cols[3], "dd/MM/yyyy", _langbr, DateTimeStyles.None, out date))
                                date = DateTime.ParseExact(cols[0], "dd/MM/yyyy", _langbr, DateTimeStyles.None);
                            if (date < startdate) continue;

                            if (!comprice.ContainsKey(date)) comprice.Add(date, currentcomprice);

                            if (dividend.ContainsKey(date))
                                dividend[date] += decimal.Parse(cols[1], _langbr);
                            else
                                dividend.Add(date, decimal.Parse(cols[1], _langbr));

                        }
                        events = comprice.OrderBy(x => x.Key).ToDictionary(x => x.Key, y => 1m);
                        comprice = comprice.OrderBy(x => x.Key).ToDictionary(x => x.Key, y => y.Value);
                        dividend = dividend.OrderBy(x => x.Key).ToDictionary(x => x.Key, y => y.Value);

                        fkeys = comprice.Keys.ToList();
                        for (var i = 0; i < fkeys.Count; i++)
                        {
                            var factor = 1 - dividend[fkeys[i]] / comprice[fkeys[i]];

                            for (var j = i + 1; j < fkeys.Count; j++)
                                factor *= (1 - dividend[fkeys[j]] / comprice[fkeys[j]]);

                            factors.Add(fkeys[i], factor);
                        }
                        factors.Add(new DateTime(2049, 12, 31), 1m);
                    }
                    catch (Exception e)
                    {
                        File.AppendAllText("exception.txt", code + ";Dividends;" + e.Message + "\r\n");
                    }
                    #endregion

                    #region Corporate events
                    try
                    {
                        index = 0;

                        while (page2.Length > 1 && (index = page2.IndexOf("<tr", index + 1)) > 1)
                        {
                            index++;
                            var idx = 0;
                            var cols = new List<string>();
                            var row = page2.Substring(index, page2.IndexOf("/tr", index) - index);
                            if (row.Contains("Cisão")) continue;

                            while ((idx = row.IndexOf("\">", idx) + 2) >= 2) cols.Add(row.Substring(idx, row.IndexOf("<", idx) - idx));

                            if (!DateTime.TryParseExact(cols[2], "dd/MM/yyyy", _langbr, DateTimeStyles.None, out date))
                                date = DateTime.ParseExact(cols[1], "dd/MM/yyyy", _langbr, DateTimeStyles.None);
                            if (date < startdate) continue;

                            cols = cols[4].Split('/').ToList();

                            var event0 = 0m;
                            if (!decimal.TryParse(cols[0], NumberStyles.Any, _langbr, out event0) || event0 <= 0) continue;

                            if (cols.Count == 1) event0 = 1 / (1m + event0 / 100m);
                            if (cols.Count == 2)
                            {
                                event0 = event0 / decimal.Parse(cols[1]);
                                if (code == 9512) event0 = .1m;
                            }

                            if (events.ContainsKey(date))
                                events[date] = event0;
                            else
                                events.Add(date, event0);
                        }
                        events.Add(new DateTime(2049, 12, 31), 1m);
                        events = events.OrderBy(x => x.Key).ToDictionary(x => x.Key, y => y.Value);
                    }
                    catch (Exception e)
                    {
                        File.AppendAllText("exception.txt", code + ";CorpEvents;" + e.Message + "\r\n");
                    }
                    #endregion

                    keys = events.Keys.ToList();
                    for (var i = 0; i < keys.Count; i++) for (var j = i + 1; j < keys.Count; j++) events[keys[i]] *= events[keys[j]];

                    keys.Except(fkeys).ToList().ForEach(k => { if (!factors.ContainsKey(k)) factors.Add(k, 0m); });

                    for (var i = 0; i < keys.Count - 1; i++) if (factors[keys[i]] == 0) factors[keys[i]] = factors[keys[i + 1]];
                    factors = factors.OrderBy(x => x.Key).ToDictionary(x => x.Key, y => y.Value);

                    #region Write to file
                    var outputfile = _leanequityfolder + @"factor_files\" + symbol.ToLower() + ".csv";
                    if (File.Exists(outputfile)) File.Delete(outputfile);

                    foreach (var key in keys)
                        File.AppendAllText(outputfile, key.ToString("yyyyMMdd") + "," +
                            Math.Round(factors[key], 9).ToString(_langus) + "," + Math.Round(events[key], 9).ToString(_langus) + "\r\n");
                    #endregion
                }
                #region Write codes and respective symbols
                var output = code.ToString("00000") + ";";
                thiscodesymbols.ForEach(s => output += s + ";");
                output = output.Substring(0, output.Length - 1) + "\r\n";
                File.AppendAllText("codes.txt", output);
                #endregion

                Console.WriteLine(DateTime.Now.TimeOfDay + " " + ((1 + codes.IndexOf(code)) / (double)codes.Count).ToString("0.00%"));
            }
            Console.WriteLine(DateTime.Now.TimeOfDay + " Done!");
        }

        private static async Task WriteNelogicaFiles(bool daily)
        {
            var dirs = new List<DirectoryInfo>();
            
            if (daily)
                dirs.Add(new DirectoryInfo(_leanequityfolder + @"daily\"));
            else
                dirs.AddRange(new DirectoryInfo(_leanequityfolder + @"minute\").GetDirectories());
            
            foreach (var dir in dirs)
            {
                var zipfiles = dir.GetFiles("*.zip");

                foreach (var zipfile in zipfiles)
                {
                    var index = zipfile.Name.IndexOf('.');
                    if (index < 4) continue;

                    var symbol = zipfile.Name.Substring(0, index);
                    var outputfile = symbol.ToUpper() + (daily ? "_Diário" : "1min") + ".csv";
                    var factors = ReadFactorFile(_leanequityfolder + @"factor_files\" + symbol + ".csv");
                    
                    File.WriteAllLines(outputfile, (await ReadZipFile(zipfile)).Select(l =>
                    {
                        var data = l.Split(',');
                        var date = DateTime.ParseExact(data[0], "yyyyMMdd", _langus);
                        var factordate = factors.Where(kvp => kvp.Key >= date).First().Key;
                        var factor = factors[factordate];

                        return symbol.ToUpper() + ";" + date.ToString("dd/MM/yyyy") + ";" +
                            Math.Round(decimal.Parse(data[1]) * factor / 10000, 2).ToString("0.00", _langbr) + ";" +
                            Math.Round(decimal.Parse(data[2]) * factor / 10000, 2).ToString("0.00", _langbr) + ";" +
                            Math.Round(decimal.Parse(data[3]) * factor / 10000, 2).ToString("0.00", _langbr) + ";" +
                            Math.Round(decimal.Parse(data[4]) * factor / 10000, 2).ToString("0.00", _langbr) + ";" + data[5];

                    }).ToArray());
                }
                Console.WriteLine("\r\n" + dir.FullName + ": " + zipfiles.Count() + " files!");
            }
            Console.WriteLine(dirs.Count + " folder(s)!" + "Done!");
        }

        private static SortedList<DateTime, decimal> ReadFactorFile(string file)
        {
            var factors = new SortedList<DateTime, decimal>();
            
            if (!File.Exists(file))
            {
                factors.Add(new DateTime(2049, 12, 31), 1m);
                return factors;
            }
       
            var lines = File.ReadAllLines(file);
            foreach (var line in lines)
            {
                var data = line.Split(',');
                var date = DateTime.ParseExact(data[0], "yyyyMMdd", _langus);
                var factor = decimal.Parse(data[1], _langus) * decimal.Parse(data[2], _langus);
                factors.Add(date, factor);
            }

            return factors;

        }

        private static async Task WriteQuantConnectMinuteFile(int minutes)
        {
            FolderCleanUp("minute");

            var dirs = new DirectoryInfo(_leanequityfolder + @"tick\").GetDirectories();

            foreach (var dir in dirs)
            {
                var zipfiles = dir.GetFiles("*.zip");

                if (zipfiles.Count() == 0) continue;
                var outdir = Directory.CreateDirectory(dir.FullName.Replace("tick", "minute"));

                foreach (var zipfile in zipfiles)
                {
                    using (var zip2open = new FileStream(zipfile.FullName, FileMode.Open, FileAccess.Read))
                    using (var archive = new ZipArchive(zip2open, ZipArchiveMode.Read))
                        foreach (var entry in archive.Entries)
                            using (var file = new StreamReader(entry.Open()))
                            {
                                var data = (await file.ReadToEndAsync()).Split('\n');

                                var line = data[0].Split(',');
                                var lon = Convert.ToInt64(10 * decimal.Parse(line[0]));
                                var ttt = new TimeSpan(Convert.ToInt64(10 * decimal.Parse(line[0])));
                                
                            }
                    var lines = File.ReadAllLines(zipfile.FullName, Encoding.ASCII).ToList();

                    var lastmin = (new DateTime()).AddMilliseconds(Convert.ToInt32(lines.Last().Split(',')[0]));

                    var currsec = (new DateTime()).AddMilliseconds(Convert.ToInt32(lines.First().Split(',')[0]));

                    currsec = currsec
                        .AddSeconds(-currsec.TimeOfDay.Seconds)
                        .AddMilliseconds(-currsec.TimeOfDay.Milliseconds);

                    while (currsec < lastmin)
                    {
                        currsec = currsec.AddMinutes(1);
                        var prev = lines.FindAll(l => (new DateTime()).AddMilliseconds(Convert.ToInt32(l.Split(',')[0])) < currsec);

                        if (prev.Count == 0) continue;

                        lines.RemoveRange(0, prev.Count);

                        var bar = currsec.AddMinutes(-1).TimeOfDay.TotalMilliseconds.ToString() + "," +
                            prev.First().Split(',')[1] + "," +
                            prev.Max(p => Convert.ToInt32(p.Split(',')[1])).ToString() + "," +
                            prev.Min(p => Convert.ToInt32(p.Split(',')[1])).ToString() + "," +
                            prev.Last().Split(',')[1] + "," +
                            prev.Sum(p => Convert.ToInt32(p.Split(',')[2])).ToString() + Environment.NewLine;

                        var newfile = zipfile.FullName.Replace("tick", "minute").Replace("Tick", "Minute");
                        File.AppendAllText(newfile, bar);

                        Console.WriteLine(DateTime.Now + ": " + newfile + " criado.");
                    }
                }

            }
        }
   
        private static async Task SearchTickerChange()
        {
            var index = 0;
            var page = string.Empty;
            var cancel = new List<string>();
            var merged = new List<string>();

            #region Get Cancelled and Incorporated companies from BmfBovespa site
            try
            {
                using (var client = new HttpClient())
                {
                    var codes = new List<string>();

                    using (var response = await client.GetAsync("http://www.bmfbovespa.com.br/cias-Listadas/empresas-com-registro-cancelado/ResumoEmpresasComRegistroCancelado.aspx?razaoSocial="))
                    using (var content = response.Content)
                    {
                        page = await content.ReadAsStringAsync();
                        if ((index = page.IndexOf("<tbody>")) >= 0) page = page.Substring(0, page.IndexOf("</tbody>")).Substring(index);

                        index = 0;
                        while (page.Length > 1 && (index = page.IndexOf("codigo=", index) + 7) > 7)
                        {
                            var code = page.Substring(index, 4);
                            if (!codes.Contains(code)) codes.Add(code);
                        }
                    }

                    foreach (var code in codes)
                    {
                        using (var response = await client.GetAsync("http://www.bmfbovespa.com.br/cias-Listadas/empresas-com-registro-cancelado/DetalheEmpresasComRegistroCancelado.aspx?codigo=" + code))
                        using (var content = response.Content)
                        {
                            page = await content.ReadAsStringAsync();
                            if ((index = page.IndexOf("lblMotivo")) < 0) { Console.Write(" " + code); continue; }
                            page = page.Substring(0, page.IndexOf("</tbody>")).Substring(index);
                            if ((index = page.IndexOf(">")) > 0) page = code + page.Substring(0, page.IndexOf("<")).Substring(index);
                            if (page.ToLower().Contains("incorporad") || code == "SUBA") merged.Add(page); else cancel.Add(page);
                        }
                    }
                    
                }
            }
            catch (Exception e) { Console.WriteLine("\r\n" + page.Substring(0, 20) + "\r\n" + e.Message); }
            #endregion
            File.WriteAllLines("Canceladas.txt", cancel.OrderBy(x => x));
            File.WriteAllLines("Incorporadas.txt", merged.OrderBy(x => x));
            cancel = cancel.Select(c => c.Substring(0, 4)).ToList();

            TickerChange.Keys.ToList().ForEach(k => { if (!cancel.Contains(k.Substring(0, 4))) cancel.Add(k.Substring(0, 4)); });
            TickerChange.Values.ToList().ForEach(k => { if (!cancel.Contains(k.Substring(0, 4))) cancel.Add(k.Substring(0, 4)); });
            cancel = cancel.Select(c => c.ToLower()).OrderBy(k => k).ToList();

            var files = new DirectoryInfo(_leanequityfolder + @"daily\").GetFiles("*.csv").ToList();
            files.RemoveAll(f => cancel.Contains(f.Name.Substring(0, 4)));
            if (files.Count == 0) return;

            var tradingdays = TradingDays();
            var firstday = new Dictionary<string, DateTime>();
            var lasttday = new Dictionary<string, DateTime>();

            foreach (var file in files)
            {            
                var data = File.ReadAllLines(file.FullName).OrderBy(d => d).ToList();
                var lday = DateTime.ParseExact(data.Last().Substring(0, 8), "yyyyMMdd", _langus);
                var fday = DateTime.ParseExact(data.First().Substring(0, 8), "yyyyMMdd", _langus);

                File.WriteAllLines(file.FullName, data);

                lasttday.Add(file.Name, lday);
                firstday.Add(file.Name, fday);                
            }
            
            foreach (var key in firstday.Keys)
            {
                var results = lasttday.Where(d => 
                    {
                        var fday = firstday[key];
                        var pday = firstday[key].AddDays(-1);
                        while (!tradingdays.Result.Contains(pday.ToString("yyyyMMdd", _langus))) pday = pday.AddDays(-1);
                        var ltfday = d.Value < fday;
                        var gtfday5 = d.Value >= pday;

                        return ltfday && gtfday5;
                    })
                    .ToDictionary(x => x.Key, y => y.Value);

                foreach (var result in results)
                {
                    var data = File.ReadAllLines(_leanequityfolder + @"daily\" + result.Key).OrderBy(d => d).ToList();
                    
                    // We count how many trading day there were between the first and the last days
                    // and calculate the frequency the symbol was traded
                    var count1 =
                        (double)(tradingdays.Result.IndexOf(data.Last().Substring(0, 8))) -
                        (double)(tradingdays.Result.IndexOf(data.First().Substring(0, 8)));

                    var freq1 = count1 == 0 ? 0 : (data.Count - 1) / count1;

                    data.RemoveAll(d => DateTime.ParseExact(d.Substring(0, 8), "yyyyMMdd", _langus) < new DateTime(result.Value.Year - 1, 1, 1));

                    var count2 =
                        (double)(tradingdays.Result.IndexOf(data.Last().Substring(0, 8))) -
                        (double)(tradingdays.Result.IndexOf(data.First().Substring(0, 8)));

                    var freq2 = count2 == 0 ? 0 : (data.Count - 1) / count2;

                    if (Math.Max(freq1, freq2) < .5) continue;

                    var output = "{\"" + key.Replace(".csv", "\", \"") + result.Key.Replace(".csv", "\"},");

                    var outvalue = string.Empty;
                    if (TickerChange.TryGetValue(key.Replace(".csv", "").ToUpper(), out outvalue))
                        if (outvalue == result.Key.Replace(".csv", "").ToUpper())
                        {
                            output += "//";
                            File.AppendAllText("mergedic.txt", output.ToUpper() + "\r\n");
                        }
                    
                    File.AppendAllText("merge.txt", output.ToUpper() + "\r\n");
                }                            
            }
            Console.WriteLine(" Done!");
        }

        private static void MergeFiles(string folder)
        {
            var files = new DirectoryInfo(_leanequityfolder + folder + @"\").GetFiles("*.csv").ToList();
            if (files.Count == 0) return;

            var symbols = TickerChange.Keys.Intersect(TickerChange.Values).ToList();
            var leg1 = TickerChange.Where(x => symbols.Contains(x.Key)).ToDictionary(x => x.Key, y => y.Value);
            var leg2 = TickerChange.Where(x => symbols.Contains(x.Value)).ToDictionary(x => x.Key, y => y.Value);
            foreach (var kvp in leg1.Concat(leg2)) TickerChange.Remove(kvp.Key);
            TickerChange = TickerChange.Concat(leg1.Concat(leg2)).ToDictionary(x => x.Key, y => y.Value);

            var mergeevent = new FileInfo("MergeEvent.txt");
            if (mergeevent.Exists) mergeevent.Delete();
            
            foreach (var kvp in TickerChange)
            {
                var newfile = files.Find(f => f.Name.Contains(kvp.Key.ToLower()));
                var oldfile = files.Find(f => f.Name.Contains(kvp.Value.ToLower()));

                if (!oldfile.Exists) continue;

                var mergeddata = File.ReadAllLines(oldfile.FullName).OrderBy(d => d).ToList();
                File.AppendAllText(mergeevent.FullName, kvp.Key + "," + kvp.Value + "," + mergeddata.Last().Substring(0, 8) + ",1\r\n");

                if (newfile.Exists) mergeddata.AddRange(File.ReadAllLines(newfile.FullName));

                File.WriteAllLines(newfile.Name, mergeddata.OrderBy(d => d));
                //File.Delete(oldfile.FullName);
            }
            foreach (var symbol in symbols) { File.Delete(symbol.ToLower() + ".csv"); }
            Console.WriteLine(" Done!");
        }

        static void ZipALLRaw(string folder)
        {
            var dirs = new List<DirectoryInfo>();
            if (folder == "daily")
                dirs.Add(new DirectoryInfo(_leanequityfolder + @"daily\"));
            else
                dirs.AddRange(new DirectoryInfo(_leanequityfolder + folder + @"\").GetDirectories());
            
            foreach (var dir in dirs)
            {
                var files = dir.GetFiles("*.csv");

                foreach (var file in files)
                {
                    var output = file.Name;

                    var alllines = File.ReadAllLines(file.FullName).OrderBy(d => d).ToList();

                    var zipfile = folder == "daily"
                        ? file.FullName.Replace(".csv", ".zip")
                        : dir.FullName + @"\" + file.Name.Substring(0, 9) + "trade.zip";

                    if (DateTime.ParseExact(alllines.Last().Substring(0, 8), "yyyyMMdd", _langus) > new DateTime(1999, 1, 1))
                    {
                        output += " zipped and";
                        File.WriteAllLines(file.FullName, alllines);

                        using (var z = new FileStream(zipfile, FileMode.Create))
                        using (var a = new ZipArchive(z, ZipArchiveMode.Create))
                            a.CreateEntryFromFile(file.FullName, file.Name);
                    }

                    Console.WriteLine(output + " deleted. Last entry: " + File.ReadAllLines(file.FullName).Last().Substring(0, 8));
                    File.Delete(file.FullName);
                }
            }
        }

        private static async Task<List<string>> TradingDays()
        {            
            var ifile = new FileInfo(_leanequityfolder.Replace("equity\\bra", "market-hours") + "holidays-bra.csv");
            if (!ifile.Exists) await WriteQuantConnectHolidayFile();

            var data = File.ReadAllLines(ifile.FullName).ToList();
            data.RemoveAt(0);   // Remove header

            var tradingdays = new List<DateTime>();
            var holidays = data.Select(d => DateTime.ParseExact(d, "yyyy, MM, dd", _langus));
            
            var date = holidays.First();
            while (date < holidays.Last()) tradingdays.Add(date = date.AddDays(1));

            tradingdays = tradingdays.Except(holidays).ToList();
            tradingdays.RemoveAll(d => d.DayOfWeek == DayOfWeek.Saturday || d.DayOfWeek == DayOfWeek.Sunday);

            return tradingdays.Select(d => d.ToString("yyyyMMdd")).ToList();
        }

        #region Utils
        private static void FolderCleanUp(string folder)
        {
            // Delete all CSV files
            var files = new DirectoryInfo(_leanequityfolder + folder + @"\").GetFiles("*.csv").ToList();
            foreach (var x in files) File.Delete(x.FullName);

            // Delete the folders
            var dires = new DirectoryInfo(_leanequityfolder + folder + @"\").GetDirectories().ToList();
            foreach (var x in dires) Directory.Delete(x.FullName, true);
        }

        private static bool Filter(string line)
        {
            int type;

            return !int.TryParse(line.Substring(16, 3), out type) || type < 3 || (type > 8 && type != 11)
                || line.Substring(12, 12).Trim().Contains(" ");
        }

        private static async Task<List<string>> ReadZipFile(FileInfo zipfile, List<string> selected)
        {
            var data = new List<string>();

            if (!zipfile.Exists)
            {
                Console.WriteLine(zipfile.Name + "does not exist!");
                return data;
            }

            try
            {
                using (var zip2open = new FileStream(zipfile.FullName, FileMode.Open, FileAccess.Read))
                using (var archive = new ZipArchive(zip2open, ZipArchiveMode.Read))
                    foreach (var entry in archive.Entries)
                        using (var file = new StreamReader(entry.Open()))
                            while (!file.EndOfStream)
                            {
                                var line = await file.ReadLineAsync();
                                if (selected.Count == 0 || selected.Any(s => line.Contains(s)))
                                    data.Add(line);
                            }
            }
            catch (Exception e) { Console.WriteLine(e.Message); }

            return data;
        }

        private static async Task<List<string>> ReadZipFile(FileInfo zipfile)
        {
            return await ReadZipFile(zipfile, selected: new List<string>());
        }
        #endregion
    }
}