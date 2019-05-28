package alchemist.util

import java.io.PrintWriter

class ConsolePrinter(consoleColorChoice: String) {

  var logWriter: PrintWriter = _
  var errWriter: PrintWriter = _

  private[this] var tabs: Int = 0

  private[this] val consoleColor: String = {
    consoleColorChoice match {
      case "cyan"           => Console.CYAN
      case "white"          => Console.WHITE
      case "black"          => Console.BLACK
      case "blue"           => Console.BLUE
      case "red"            => Console.RED
      case "green"          => Console.GREEN
      case "yellow"         => Console.YELLOW
      case "magenta"        => Console.MAGENTA
      case "bright cyan"    => Console.BOLD + Console.CYAN
      case "bright white"   => Console.BOLD + Console.WHITE
      case "bright black"   => Console.BOLD + Console.BLACK
      case "bright blue"    => Console.BOLD + Console.BLUE
      case "bright red"     => Console.BOLD + Console.RED
      case "bright green"   => Console.BOLD + Console.GREEN
      case "bright yellow"  => Console.BOLD + Console.YELLOW
      case "bright magenta" => Console.BOLD + Console.MAGENTA
      case _                => Console.BOLD + Console.WHITE
    }
  }

  def setLogWriter(_logWriter: PrintWriter) {
    logWriter = _logWriter
  }

  def setErrWriter(_errWriter: PrintWriter) {
    errWriter = _errWriter
  }

  def tab: Unit = { tabs = 10.min(tabs + 1) }

  def untab: Unit = { tabs = 0.max(tabs - 1) }

  def pad: String = {
    var consoleTab = ""
    (1 to tabs).map(_ => consoleTab += "    ")
    consoleTab
  }

  def println(text: Any, args: Any*) {
    if (logWriter != null) logWriter.write(pad + text.toString + "\n")
    printf(pad + consoleColor + text.toString + Console.RESET + "\n")
  }

  def printError(text: Any, args: Any*) {
    if (errWriter != null) errWriter.write(pad + text.toString + "\n")
    printf(pad + consoleColor + text.toString + Console.RESET + "\n")
  }
}

object ConsolePrinter {

  def apply(consoleColorChoice: String) = {
    new ConsolePrinter(consoleColorChoice)
  }
}
