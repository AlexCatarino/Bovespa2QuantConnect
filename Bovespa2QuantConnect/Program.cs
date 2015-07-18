using System;
using System.IO.Compression;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Globalization;
using System.Net.Http;

namespace Bovespa2QuantConnect
{
    class Program
    {
        static readonly CultureInfo _langbr = CultureInfo.CreateSpecificCulture("pt-BR");
        static readonly CultureInfo _langus = CultureInfo.CreateSpecificCulture("en-US");
        static readonly string _leanequityfolder = @"C:\Users\Alexandre\Documents\GitHub\Lean\Data\equity\brazil\";

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

            do
            {
                key = Console.ReadKey().KeyChar;
                var dir = @"C:\Users\Alexandre\Documents\IBOV\Stock\";

                switch (key)
                {
                    case '1':
                        ReadZipFiles(new DirectoryInfo(dir).GetFiles("COTAHIST_A2*zip"), false);
                        break;
                    case '2':
                        ReadZipFiles(new DirectoryInfo(dir).GetFiles("NEG_20*zip"), false);
                        break;
                    case '3':
                        MakeMinuteFilesFromTickFiles();
                        break;
                    case '4':
                        WriteQuantConnectFactorFiles();
                        break;
                    case '5':
                        WriteNelogicaFiles();
                        break;
                    case '6':
                        //new string[] 
                        //{ 
                        //    //"ITUB4", "GGBR4", "CYRE3", "KROT3", "HGTX3", "MRFG3",  
                        //    "BBAS3"//,"JBSS3",  "POMO4", "USIM5", "ALLL3",
                        //}.ToList().ForEach(s => AdjustedPrice(s, new DateTime(2014, 5, 31)));
                        break;
                    case '7':
                        //ReadNEG(true, cutoff);
                        break;
                    case '8':
                        ReadZipFiles(new DirectoryInfo(dir).GetFiles("COTAHIST_A2*zip"), true);
                        break;
                    case '9':
                        ZipALLRaw();
                        break;
                    default:
                        Console.WriteLine("\nOpção Inválida!\n" + menu);
                        break;
                }

            } while (key != '0');
        }

        private static async Task ReadZipFiles(FileInfo[] zipfiles, bool iswriteholidayfiles)
        {
            foreach (var zipfile in zipfiles)
            {
                using (var zip2open = new FileStream(zipfile.FullName, FileMode.Open, FileAccess.Read))
                {
                    using (var archive = new ZipArchive(zip2open, ZipArchiveMode.Read))
                    {
                        foreach (var entry in archive.Entries)
                        {
                            if (iswriteholidayfiles)
                            {
                                await WriteQuantConnectHolidayFile(entry);
                            }
                            else
                            {
                                using (var file = new StreamReader(entry.Open()))
                                {
                                    while (!file.EndOfStream)
                                    {
                                        var line = await file.ReadLineAsync();

                                        if (zipfile.Name.Contains("NEG"))
                                            await WriteQuantConnectTickFile(line);
                                        else
                                            await WriteQuantConnectDailyFile(line);
                                    }
                                }
                            }
                        }
                    }
                }
                Console.WriteLine("> " + zipfile);
            }
        }

        private static async Task WriteQuantConnectHolidayFile(ZipArchiveEntry entry)
        {
            // America/Sao_Paulo,brazil,,equity,-,-,-,-,9.5,10,16.9166667,18,9.5,10,16.9166667,18,9.5,10,16.9166667,18,9.5,10,16.9166667,18,9.5,10,16.9166667,18,-,-,-,-
            var year = int.Parse(entry.Name.Substring(10, 4));
            var filedate = new DateTime(year + 1, 1, 1).AddDays(-1);
            var lastdate = new DateTime(1999, 1, 1);
            var outputfile = _leanequityfolder.Replace("equity\brazil", "market-hours") + "holidays-brazil.csv";
            var fileexists = File.Exists(outputfile) && 
                DateTime.TryParseExact(File.ReadAllLines(outputfile).ToList().Last(), "yyyy, MM, dd",
                CultureInfo.InvariantCulture, DateTimeStyles.None, out lastdate);

            if (lastdate >= filedate) return;
            if (!fileexists) File.WriteAllText(outputfile, "year, month, day\r\n# Brazil Equity market holidays\r\n");
            
            var holidays = new List<DateTime>();

            using (var file = new StreamReader(entry.Open()))
            {
                filedate = new DateTime(year, 1, 1);
            
                while (filedate.Year == year)
                {
                    if (filedate.DayOfWeek != DayOfWeek.Saturday && filedate.DayOfWeek != DayOfWeek.Sunday) holidays.Add(filedate);
                    filedate = filedate.AddDays(1);
                }

                while (!file.EndOfStream)
                {
                    var line = await file.ReadLineAsync();
                    if (DateTime.TryParseExact(line.Substring(2, 8), "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out filedate))
                        holidays.Remove(filedate);
                }

                holidays.RemoveAll(d => d <= lastdate);
            }

            if (holidays.Last().Year == DateTime.Now.Year - 1)
            {
                var ids = new List<int>();
                var url = "http://www.bmfbovespa.com.br/pt-br/regulacao/calendario-do-mercado/calendario-do-mercado.aspx";
                var months = new string[] { "Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez" }.ToList();

                try
                {
                    using (var client = new HttpClient())
                    using (var response = await client.GetAsync(url))
                    using (var content = response.Content)
                    {
                        var i = 0;
                        var id = 0;
                        var page = await content.ReadAsStringAsync();
                        page = page.Substring(0, page.IndexOf("linhaDivMais"));

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
                                    "ddMMMyyyy", _langbr, DateTimeStyles.None, out filedate))
                                    holidays.Add(filedate);                
                            }
                            
                            i++;
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }

                holidays.ForEach(d => File.AppendAllText(outputfile, d.Year.ToString("0000") + ", " + d.Month.ToString("00") + ", " + d.Day.ToString("00\r\n")));                
            }
        }

        private static async Task WriteQuantConnectTickFile(string line)
        {
            int type;
            if (!int.TryParse(line.Substring(15, 3), out type) || type > 11) return;

            line = line
                .Replace("BMEF", "BVMF")
                .Replace("TLPP", "VIVT")
                .Replace("VNET", "CIEL")
                .Replace("VCPA", "FIBR")
                .Replace("PRGA", "BRFS")
                .Replace("AMBV4", "ABEV3")
                .Replace("DURA4", "DTEX3");

            var data = line.Split(';');

            var symbol = data[1].Trim().ToLower();
            if (symbol != "bbas3") return;

            var dir = Directory.CreateDirectory(_leanequityfolder + @"tick\" + symbol + @"\");

            var csvfile = dir.FullName + data[0].Replace("-", "") + "_" + symbol + "_Trade_Tick.csv";

            var output = TimeSpan.Parse(data[5]).TotalMilliseconds.ToString() + ",";
            output += (Decimal.Parse(data[3].Replace(".", ",")) * 10000m).ToString("#.") + ",";
            output += Int64.Parse(data[4]) + Environment.NewLine;

            File.AppendAllText(csvfile, output);
        }

        private static async Task WriteQuantConnectDailyFile(string line)
        {
            int type;
            if (!int.TryParse(line.Substring(16, 3), out type) || type > 11) return;

            line = line
                .Replace("BMEF", "BVMF")
                .Replace("TLPP", "VIVT")
                .Replace("VNET", "CIEL")
                .Replace("VCPA", "FIBR")
                .Replace("PRGA", "BRFS")
                .Replace("AMBV4", "ABEV3")
                .Replace("DURA4", "DTEX3");

            var dir = Directory.CreateDirectory(_leanequityfolder + @"daily\");
            var file = dir.FullName + line.Substring(12, 12).Trim().ToLower() + ".csv";

            var output = line.Substring(2, 8) + ",";
            output += Convert.ToInt64(line.Substring(56, 13)) * 100 + ",";
            output += Convert.ToInt64(line.Substring(69, 13)) * 100 + ",";
            output += Convert.ToInt64(line.Substring(82, 13)) * 100 + ",";
            output += Convert.ToInt64(line.Substring(108, 13)) * 100 + ",";
            output += Convert.ToInt64(line.Substring(152, 18)) + "\r\n";

            File.AppendAllText(file, output);
        }

        private static async Task WriteQuantConnectFactorFiles()
        {
            var codes = new List<int>();
            var symbols = new List<string>();
            var alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".ToList();
            alphabet.Clear();

            #region GetCodes
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
            codes = codes.OrderBy(i => i).ToList();
            #endregion

            #region GetSymbols
            new DirectoryInfo(_leanequityfolder + @"daily\").GetFiles("*.zip").ToList().ForEach(z =>
                {
                    var symbol = z.Name.Replace(".zip", "").ToUpper();
                    if (symbol.Length > 3 && !symbols.Contains(symbol)) symbols.Add(symbol);
                });
            #endregion

            codes.Add(4170);
            codes.Add(9512);

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

                var thiscodesymbols = symbols.Intersect(page0.Split(',')).ToList();
                var kind = new Dictionary<int, string> { { 3, "ON" }, { 4, "PN" }, { 5, "PNA" }, { 6, "PNB" }, { 7, "PNC" }, { 8, "PND" }, { 11, "UNT" } };

                foreach (var symbol in thiscodesymbols)
                {
                    var date = new DateTime();

                    var events = new Dictionary<DateTime, decimal>();
                    var factors = new Dictionary<DateTime, decimal>();

                    var dividend = new Dictionary<DateTime, decimal>();
                    var comprice = new Dictionary<DateTime, decimal>();

                    #region Dividends
                    index = 0;

                    while ((index = page1.IndexOf(">" + kind[int.Parse(symbol.Substring(4))] + "<", index)) > 0)
                    {
                        index++;
                        var idx = 0;
                        var cols = new List<string>();
                        var row = page1.Substring(index, page1.IndexOf("</tr>", index) - index);

                        while ((idx = row.IndexOf("\">", idx) + 2) >= 2) cols.Add(row.Substring(idx, row.IndexOf("<", idx) - idx));

                        if (!DateTime.TryParseExact(cols[4], "dd/MM/yyyy", _langbr, DateTimeStyles.None, out date) &&
                            !DateTime.TryParseExact(cols[3], "dd/MM/yyyy", _langbr, DateTimeStyles.None, out date))
                            date = DateTime.ParseExact(cols[0], "dd/MM/yyyy", _langbr, DateTimeStyles.None);
                        if (date < new DateTime(2000, 1, 1)) continue;

                        if (!comprice.ContainsKey(date)) comprice.Add(date, decimal.Parse(cols[5], _langbr));

                        if (dividend.ContainsKey(date))
                            dividend[date] += decimal.Parse(cols[1], _langbr);
                        else
                            dividend.Add(date, decimal.Parse(cols[1], _langbr));

                    }
                    events = comprice.OrderBy(x => x.Key).ToDictionary(x => x.Key, y => 1m);
                    comprice = comprice.OrderBy(x => x.Key).ToDictionary(x => x.Key, y => y.Value);
                    dividend = dividend.OrderBy(x => x.Key).ToDictionary(x => x.Key, y => y.Value);
                    
                    var fkeys = comprice.Keys.ToList();
                    for (var i = 0; i < fkeys.Count; i++)
                    {
                        var factor = 1 - dividend[fkeys[i]] / comprice[fkeys[i]];

                        for (var j = i + 1; j < fkeys.Count; j++)
                            factor *= (1 - dividend[fkeys[j]] / comprice[fkeys[j]]);

                        factors.Add(fkeys[i], factor);
                    }
                    factors.Add(new DateTime(2049, 12, 31), 1m);
                    #endregion

                    #region Corporate events
                    index = 0;

                    while ((index = page2.IndexOf("<tr", index + 1)) > 1)
                    {
                        index++;
                        var idx = 0;
                        var cols = new List<string>();
                        var row = page2.Substring(index, page2.IndexOf("/tr", index) - index);
                        if (row.Contains("Cisão")) continue;

                        while ((idx = row.IndexOf("\">", idx) + 2) >= 2) cols.Add(row.Substring(idx, row.IndexOf("<", idx) - idx));

                        if (!DateTime.TryParseExact(cols[2], "dd/MM/yyyy", _langbr, DateTimeStyles.None, out date))
                            date = DateTime.ParseExact(cols[1], "dd/MM/yyyy", _langbr, DateTimeStyles.None);
                        if (date < new DateTime(2000, 1, 1)) continue;

                        cols = cols[4].Split('/').ToList();

                        var event0 = decimal.Parse(cols[0]);
                        if (cols.Count == 1) event0 = 1 / (1m + event0 / 100m);
                        if (cols.Count == 2) event0 = event0 / decimal.Parse(cols[1]);

                        if (events.ContainsKey(date))
                            events[date] = event0;
                        else
                            events.Add(date, event0);                      
                    }
                    events.Add(new DateTime(2049, 12, 31), 1m);
                    events = events.OrderBy(x => x.Key).ToDictionary(x => x.Key, y => y.Value);
                    #endregion

                    var keys = events.Keys.ToList();
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
            }
            Console.WriteLine("Done!");
        }

        private static async Task WriteNelogicaFiles()
        {
            var factorfilesdir = _leanequityfolder + @"factor_files\";
            var dirs = new DirectoryInfo(_leanequityfolder + @"minute\").GetDirectories().ToList();
            dirs.Clear();
            dirs.Add(new DirectoryInfo(_leanequityfolder + @"daily\"));
            Console.WriteLine();
            
            foreach (var dir in dirs)
            {
                var zipfiles = dir.GetFiles("*.zip");
 
                foreach (var zipfile in zipfiles)
                {
                    var factorfile = factorfilesdir + zipfile.Name.Replace("zip", "csv");

                    // Check if we have the file with the factors
                    if (!File.Exists(factorfile) || zipfile.Name.Length < 9) continue;

                    var factors = ReadFactorFile(factorfile);                        
                    
                    using (var zip2open = new FileStream(zipfile.FullName, FileMode.Open, FileAccess.Read))
                    using (var archive = new ZipArchive(zip2open, ZipArchiveMode.Read))
                    using (var file = new StreamReader(archive.Entries.First().Open()))
                        while (!file.EndOfStream)
                        {
                            var line = await file.ReadLineAsync();

                        }
                }
            }
            
        }
        private static SortedList<DateTime,decimal> ReadFactorFile(string file)
        {
            var factors = new SortedList<DateTime, decimal>();

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

        private static async Task MakeMinuteFilesFromTickFiles()
        {
            Console.WriteLine();
            var dirs = new DirectoryInfo(_leanequityfolder + @"tick\").GetDirectories();

            foreach (var dir in dirs)
            {
                var files = dir.GetFiles("*.csv");

                if (files.Count() == 0) continue;
                var outdir = Directory.CreateDirectory(dir.FullName.Replace("tick", "minute"));

                foreach (var file in files)
                {
                    var lines = File.ReadAllLines(file.FullName, Encoding.ASCII).ToList();

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

                        var newfile = file.FullName.Replace("tick", "minute").Replace("Tick", "Minute");
                        File.AppendAllText(newfile, bar);

                        Console.WriteLine(DateTime.Now + ": " + newfile + " criado.");
                    }
                }

            }
        }

        static void ZipALLRaw()
        {
            Console.WriteLine();
            var dirs = new DirectoryInfo(_leanequityfolder + @"minute\").GetDirectories().ToList();
            dirs.AddRange(new DirectoryInfo(_leanequityfolder + @"tick\").GetDirectories().ToList());
            dirs.Clear();
            dirs.Add(new DirectoryInfo(_leanequityfolder + @"daily\"));

            foreach (var dir in dirs)
            {
                var files = dir.GetFiles("*.csv");

                foreach (var file in files)
                {
                    var zipfile = dir.FullName + @"\" + file.Name.Substring(0, 9) + "trade.zip";
                    if (dir.FullName.Contains("daily")) zipfile = file.FullName.Replace(".csv", ".zip");

                    using (var z = new FileStream(zipfile, FileMode.Create))
                    using (var a = new ZipArchive(z, ZipArchiveMode.Create))
                        a.CreateEntryFromFile(file.FullName, file.Name);

                    //
                    File.Delete(file.FullName);
                    Console.WriteLine(file.Name + " zipped and deleted");
                }


            }

        }
    }
}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                