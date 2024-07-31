#![feature(path_file_prefix)]

use std::fs::read_dir;
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use std::ffi::OsStr;
use std::ffi::OsString;
use std::thread;
use std::time::{Duration, Instant};

use clap::{Args, Parser, Subcommand, ValueEnum};
use crossbeam_channel::bounded;
use env_logger;
use log::{error, info};
use logfile2btreefile::{conversion, db_test};
use pi_async_file::file::rename;
use pi_async_rt::rt::startup_global_time_loop;
use pi_async_rt::rt::{multi_thread::MultiTaskRuntimeBuilder, AsyncRuntime};

mod logfile2btreefile;

#[macro_use]
extern crate lazy_static;

#[derive(Debug, Parser)]
#[command(name = "tools")]
#[command(about = "数据库转换工具", long_about = None)]
struct Cli {
    #[command(subcommand)]
    table_conversion: TableConversion,
}

#[derive(Debug, Subcommand)]
enum TableConversion {
    /// logfile转btreefile
    #[command(arg_required_else_help = true)]
    Logfile2btreefile {
        /// 待转换路径
        src: String,
        /// 目标路径
        out: String,
        /// 单次处理数量
        #[clap(short, long, default_value_t = 10000)]
        batch: usize,
    },
    /// 加载数据库并打印表和记录数
    #[command(arg_required_else_help = true)]
    Test {
        /// 数据库路径
        path: String,
    },
}

fn main() {
    //启动日志系统
    // env_logger::builder().format_timestamp_millis().init();
    env_logger::init();
    let _handle = startup_global_time_loop(100);

    let args = Cli::parse();
    match args.table_conversion {
        TableConversion::Logfile2btreefile { src, out, batch } => {
            println!("Logfile2Btreefile {src} {out}");
            let start = Instant::now();
            let r = conversion(src, out, batch);

            println!("conversion r:{:?}, time:{:?}", r, start.elapsed());
        }
        TableConversion::Test { path } => {
            println!("r:{:?}", db_test(path));
        }
    }
    // thread::sleep(Duration::from_millis(1000000000));
}
