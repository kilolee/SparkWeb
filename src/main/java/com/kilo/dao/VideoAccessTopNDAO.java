package com.kilo.dao;

import com.kilo.entity.VideoAccessTopN;
import com.kilo.utils.MySQLUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 面向实体编程
 * Created by kilo on 2018/3/20.
 */
public class VideoAccessTopNDAO {

    static Map<String, String> courses = new HashMap<String, String>();

    static {
        courses.put("4000", "MySQL优化");
        courses.put("4500", "Crontab");
        courses.put("4600", "Swift");
        courses.put("14540", "SpringData");
        courses.put("14704", "R");
        courses.put("14390", "机器学习");
        courses.put("14322", "redis");
        courses.put("14623", "Docker");
    }

    public String getCourseName(String id) {
        return courses.get(id);
    }

    /**
     * 根据day查询当天的最受欢迎的Top5课程
     *
     * @param day
     * @return
     */
    public List<VideoAccessTopN> query(String day) {
        List<VideoAccessTopN> list = new ArrayList<VideoAccessTopN>();
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            connection = MySQLUtils.getConnection();
            String sql = "select cms_id , times from day_video_access_topn_stat where day = ? order by times desc limit 5";
            pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, day);
            rs = pstmt.executeQuery();

            VideoAccessTopN entity = null;
            while (rs.next()) {
                entity = new VideoAccessTopN();
                entity.setName(getCourseName(rs.getLong("cms_id") + ""));
                entity.setValue(rs.getLong("times"));
                list.add(entity);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            MySQLUtils.release(connection, pstmt, rs);
        }
        return list;
    }

    public static void main(String[] args) {
        VideoAccessTopNDAO dao = new VideoAccessTopNDAO();
        List<VideoAccessTopN> list = dao.query("20170511");
        for (VideoAccessTopN result : list) {
            System.out.println(result.getName() + " ," + result.getValue());
        }
    }


}
