#![feature(path_file_prefix)]

use std::fs::read_dir;
use std::str::FromStr;
use std::path::PathBuf;
use std::io::{Error, Result, ErrorKind};

use clap::{App, Arg, ArgMatches, SubCommand};
use pi_async_rt::rt::{AsyncRuntime, multi_thread::MultiTaskRuntimeBuilder};
use pi_async_file::file::rename;
use crossbeam_channel::bounded;
use log::{info, error};
use env_logger;

fn main() {
    //启动日志系统
    env_logger::builder().format_timestamp_millis().init();

    //匹配启动时的选项和参数
    let matches = App::new("LogFile Version Upgrade Tools")
        .version("0.1.0")
        .author("YiNeng <yineng@foxmail.com>")
        .arg(
            Arg::with_name("LOG_FILES_PATH") //原始版本的LogFile所在路径，支持递归遍历
                .help("Path of LogFile")
                .required(true)
                .index(1),
        )
        .get_matches();

    //获取和解析LogFile根路径
    let root = if let Some(path) = matches.value_of("LOG_FILES_PATH") {
        match PathBuf::from_str(path) {
            Err(e) => panic!("Parse LogFile path failed, path: {:?}, reason: {:?}", path, e),
            Ok(path) => {
                path
            }
        }
    } else {
        //未设置必须的参数
        panic!("Take LogFile path failed, reason: Require set LOG_FILES_PATH");
    };

    //获取指定目录下的所有LogFile目录
    let mut log_file_paths = Vec::default();
    if let Err(e) =  read_paths(root, &mut log_file_paths) {
        panic!("{:?}", e);
    }
    log_file_paths.sort();

    //异步并发处理原始版本的LogFile目录内的日志文件
    let rt = MultiTaskRuntimeBuilder::default().build();
    let mut map = rt.map_reduce(log_file_paths.len());
    for path in log_file_paths {
        let rt_copy = rt.clone();
        map.map(rt.clone(), async move {
            if let Some(parent) = path.parent() {
                if let Some(file_name) = path.file_name() {
                    if let Some(file_name_str) = file_name.to_str() {
                        let vec: Vec<&str> = file_name_str.split('.').collect();
                        if let Ok(log_file_index) = vec[0].parse::<usize>() {
                            let new_file_name_str = create_log_file_name(9, log_file_index);
                            let mut new_path_str = parent.join(new_file_name_str).to_str().unwrap().to_string();
                            for index in 1..vec.len() {
                                new_path_str = new_path_str + "." + vec[index];
                            }

                            //更新LogFile的文件名
                            if let Err(e) = rename(rt_copy, path.clone(), PathBuf::from(new_path_str.clone())).await {
                                error!("Rename LogFile name failed, from: {:?}, to: {:?}, reason: {:?}", path, new_path_str, e);
                            } else {
                                info!("Rename LogFile name ok, from: {:?}, to: {:?}", path, new_path_str);
                            }
                        }
                    }
                }
            }

            Ok(())
        });
    }

    let (sender, receiver) = bounded(1);
    rt.spawn(rt.alloc(), async move {
        match map.reduce(false).await {
            Err(e) => {
                error!("Mapreduce failed, reason: {:?}", e);
            },
            Ok(result) => {
                //改名成功，则立即通知
                sender.send(result.len());
            }
        }
    });

    match receiver.recv() {
        Err(e) => {
            panic!("Receive failed, reason: {:?}", e);
        },
        Ok(len) => {
            info!("Rename LogFile ok, len: {}", len);
        },
    }
}

// 递归遍历指定目录下的所有LogFile目录
fn read_paths(current: PathBuf,
              paths: &mut Vec<PathBuf>) -> Result<()> {
    match read_dir(current.as_path()) {
        Err(e) => {
            Err(Error::new(ErrorKind::Other,
                           format!("Read paths failed, path: {:?}, reason: {:?}", current, e)))
        },
        Ok(mut dirs) => {
            while let Some(dir) = dirs.next() {
                match dir {
                    Err(e) => {
                        return Err(Error::new(ErrorKind::Other,
                                              format!("Read paths failed, path: {:?}, reason: {:?}", current, e)));
                    },
                    Ok(dir) => {
                        let path = dir.path();
                        if path.is_dir() {
                            //当前文件是目录，则继续遍历当前目录下的所有目录
                            read_paths(path.clone(), paths);
                        }
                        paths.push(path);
                    },
                }
            }

            Ok(())
        },
    }
}

//生成指定宽度的日志文件名
#[inline]
fn create_log_file_name(width: usize, id: usize) -> String {
    format!("{:0>width$}", id, width = width)
}

